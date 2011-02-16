require 'java'
require File.dirname(__FILE__) + '/commons-cli-1.1.jar'
require File.dirname(__FILE__) + '/commons-io-1.2.jar'
require File.dirname(__FILE__) + '/rabbitmq-client.jar'
require File.dirname(__FILE__) + '/rabbitmq_client.rb'
require File.dirname(__FILE__) + '/rabbitmq_queue.rb'
require File.dirname(__FILE__) + '/rabbitmq_rpc_server.rb'

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
    def initialize(channel, block, marshaller=nil)
      @channel = channel
      @block = block
      @marshaller = (marshaller == false) ? DefaultMarshaller : marshaller
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
    
    def initialize(name, type, channel, durable=false, auto_delete=false)
      @name = name
      @type = type
      @durable = durable
      @channel = channel
      @auto_delete = auto_delete
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
    def select_marshaller(m)
      case m
      when false
        nil
      when nil
        DefaultMarshaller
      else
        raise RabbitMQClientError, "invalid marshaller" unless (m.nil? or (m.respond_to? :load and m.respond_to? :dump))
        m
      end
    end
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
    @marshaller = RabbitMQClient.select_marshaller(options[:marshaller])
    
    # queues and exchanges
    @queues = {}
    @exchanges = {}
    @rpc_servers = {}
    @rpc_clients = {}
    
    connect unless options[:no_auto_connect]
    # Disconnect before the object is destroyed
    define_finalizer(self, lambda {|id| self.disconnect if self.connected? })
    self
  end

  def marshaller=(marshaller)
    @marshaller = RabbitMQClient.select_marshaller(marshaller)
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
    @rpc_servers.values.each { |s| s.close }
    @queues.values.each { |q| q.unbind }
    @channel.close
    @connection.close
    @connection = nil
  end
  
  def connected?
    @connection != nil
  end
  
  def queue(name, durable=false, marshaller=nil)
    marsh = RabbitMQClient.select_marshaller(marshaller)
    @queues[name] ||= Queue.new(name, @channel, durable, marsh)
  end
  
  def exchange(name, type='fanout', durable=false, auto_delete=false)
    @exchanges[name] ||= Exchange.new(name, type, @channel, durable, auto_delete)
  end
  
  #def find_queue(name)
  #  begin
  #    dok = @channel.queueDeclarePassive(name)
  #    dok.getQueue
  #end
  
  def rpc_server(name=nil, exchange=nil, routing_key='', marshaller=nil, &block)
    marsh = RabbitMQClient.select_marshaller(marshaller)
    bl = block_given? ? block : nil
    svr = RabbitMQRpcServer.new(@channel, name, exchange, routing_key, marsh, bl)
    @rpc_servers[svr.queue_name] = svr
    svr.start
    svr
  end
  
  def delete_rpc_server(svr)
    if svr.kind_of?(String)
      s = @rpc_servers[svr] ? [svr, @rpc_servers[svr]] : nil
    else
      s = @rpc_servers.rassoc(svr)
    end
    if s
      s[1].unbind_queue
      s[1].close
      @rpc_servers.delete(s[0])
    end
  end
  
  def rpc_client(name, exchange, routing_key='', marshaller=nil)
    marsh = RabbitMQClient.select_marshaller(marshaller)
    client = RabbitMQRpcClient.new(@channel, name, exchange, routing_key, marsh)
    @rpc_clients[client.name] = client
    client
  end
  
  def delete_rpc_client(client)
    if client.kind_of?(String)
      s = @rpc_clients[client] ? [client, @rpc_clients[client]] : nil
    else
      s = @rpc_clients.rassoc(client)
    end
    if s
      s[1].close
      @rpc_clients.delete(s[0])
    end
  end
end

