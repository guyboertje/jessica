require 'java'
require File.dirname(__FILE__) + '/commons-cli-1.1.jar'
require File.dirname(__FILE__) + '/commons-io-1.2.jar'
require File.dirname(__FILE__) + '/rabbitmq-client.jar'

class RabbitMQClient
  include ObjectSpace
  include_class('com.rabbitmq.client.Connection')
  include_class('com.rabbitmq.client.ConnectionFactory')
  include_class('com.rabbitmq.client.Channel')
  include_class('com.rabbitmq.client.Consumer')
  include_class('com.rabbitmq.client.DefaultConsumer')
  include_class('com.rabbitmq.client.QueueingConsumer')
  include_class('com.rabbitmq.client.MessageProperties')
  include_class('java.lang.InterruptedException')
  include_class('java.lang.String') { |package, name| "J#{name}" }
  
  class RabbitMQClientError < StandardError;end
  
  class DefaultMarshaller
    def self.load(body)
      Marshal.load(String.from_java_bytes(body))
    end
    def self.dump(message_body)
      Marshal.dump(message_body).to_java_bytes
    end
  end
  
  class QueueConsumer < DefaultConsumer
    def initialize(channel, block, marshaller=DefaultMarshaller)
      @channel = channel
      @block = block
      @marshaller = marshaller
      super(channel)
    end
    
    def handleDelivery(consumer_tag, envelope, properties, body)
      delivery_tag = envelope.get_delivery_tag
      message_body = @marshaller.nil? ? body : @marshaller.send(:load, body)
      # TODO: Do we need to do something with properties?
      case @block.arity
      when 1
        @block.call message_body
      when 2
        @block.call message_body, properties
      when 3
        @block.call message_body, properties, envelope
      end
      @channel.basic_ack(delivery_tag, false)
    end
  end
  
  class Exchange
    attr_reader :name
    attr_reader :durable
    
    def initialize(name, type, channel, durable=false)
      @name = name
      @type = type
      @durable = durable
      @channel = channel
      auto_delete = false
      # Declare a non-passive, auto-delete exchange
      @channel.exchange_declare(@name, type.to_s, durable, auto_delete, nil)
      self
    end
    
    def delete
      @channel.exchange_delete(@name)
    end
  end
  
  # Class Methods
  class << self
  end
  
  attr_reader :marshaller
  attr_reader :channel
  attr_reader :connection
  
  # Instance Methods
  def initialize(options={})
    # server address
    @host = options[:host] || '127.0.0.1'
    @port = options[:port] || 5672
    
    # login details
    @username = options[:username] || 'guest'
    @password = options[:password] || 'guest'
    @vhost = options[:vhost] || '/'
    
    #marshalling
    case options[:marshaller]
    when false
      @marshaller = nil
    when nil
      @marshaller = DefaultMarshaller
    else
      @marshaller = options[:marshaller]
    end
    raise RabbitMQClientError, "invalid marshaller" unless (@marshaller.nil? or (@marshaller.respond_to? :load and @marshaller.respond_to? :dump))
    
    # queues and exchanges
    @queues = {}
    @exchanges = {}
    
    connect unless options[:no_auto_connect]
    # Disconnect before the object is destroyed
    define_finalizer(self, lambda {|id| self.disconnect if self.connected? })
    self
  end

  def marshaller=(marshaller)
    case marshaller
    when false
      @marshaller = nil
    when nil
      @marshaller = DefaultMarshaller
    else
      raise RabbitMQClientError, "invalid marshaller" unless (marshaller.nil? or (marshaller.respond_to? :load and marshaller.respond_to? :dump))
      @marshaller = marshaller
    end
  end
  
  def connect
    conn_factory = ConnectionFactory.new
    conn_factory.set_username(@username)
    conn_factory.set_password(@password)
    conn_factory.set_virtual_host(@vhost)
    conn_factory.set_requested_heartbeat(0)
    conn_factory.set_host(@host)
    conn_factory.set_port(@port)
    @connection = conn_factory.new_connection
    @channel = @connection.create_channel
  end
  
  def disconnect
    @queues.values.each { |q| q.unbind }
    @channel.close
    @connection.close
    @connection = nil
  end
  
  def connected?
    @connection != nil
  end
  
  def queue(name, durable=false, marshaller=false)
    marsh = (marshaller == false) ? @marshaller : marshaller
    @queues[name] ||= Queue.new(name, @channel, durable, marsh)
  end
  
  def exchange(name, type='fanout', durable=false)
    @exchanges[name] ||= Exchange.new(name, type, @channel, durable)
  end
end

