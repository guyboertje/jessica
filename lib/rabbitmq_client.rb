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
  include_class('com.rabbitmq.client.ReturnListener')
  include_class('com.rabbitmq.client.ConfirmListener')
  include_class('java.lang.InterruptedException')
  include_class('java.lang.String') { |package, name| "J#{name}" }


  class RabbitMQClientError < StandardError;end

  class ReturnedMessageListener
    include ReturnListener

    def initialize(callable)
      @callable = callable
    end

    def handleReturn(reply_code, reply_text, exchange, routing_key, properties, body)
      body_ = String.from_java_bytes(body)
      ret = {:kind => 'RETURN',:reply_code => reply_code, :reply_text => reply_text, :exchange => exchange, :routing_key => routing_key, :properties => properties, :body => body_}
      @callable.call(ret)
    end
  end

  class ConfirmedMessageListener
    include ConfirmListener

    def initialize(callable)
      @callable = callable
    end

    def handleAck(delivery_tag,multiple) #long,boolean
      ret = {:kind => 'ACK',:delivery_tag => delivery_tag, :multiple => multiple}
      @callable.call(ret)
    end

    def handleNack(delivery_tag,multiple) #long,boolean
      ret = {:kind => 'NACK',:delivery_tag => delivery_tag, :multiple => multiple}
      @callable.call(ret)
    end
  end

  class QueueConsumer < DefaultConsumer
    def initialize(channel, block)
      @channel = channel
      @block = block
      super(channel)
    end

    def handleDelivery(consumer_tag, envelope, properties, body)
      delivery_tag = envelope.get_delivery_tag
      message_body = String.from_java_bytes(body)
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

  class Queue
    attr_reader :name

    def initialize(name, channel, opts={})
      @name = name
      @durable,@auto_delete,@args = opts.values_at(:durable,:auto_delete,:args)
      @channel = channel
      exclusive = false
      @bindings = {}
      @channel.queue_declare(name, @durable, exclusive, @auto_delete || false, @args)
      self
    end

    def match_opts(opts)
      dur,aut,arg = opts.values_at(:durable,:auto_delete,:args)
      @durable == dur && @auto_delete == aut && @args == arg
    end

    #@queue.bind @exchange, :routing_key => 'messages.#'
    def bind(exchange, opts)
      key = opts[:routing_key]
      @bindings[exchange] = opts
      @channel.queue_bind(@name, exchange.name, key)
      self
    end

    def unbind(exchange, opts)
      key = opts[:routing_key]
      @channel.queue_unbind(@name, exchange.name, key)
    end

    def retrieve(opts={})
      message_body = nil
      no_ack = opts[:no_ack] ? opts[:no_ack] : false
      response = @channel.basic_get(@name, no_ack)
      if response
        props = response.props
        message_body = String.from_java_bytes(response.body)
        delivery_tag = response.envelope.delivery_tag
        @channel.basic_ack(delivery_tag, false)
      end
      unless opts.empty?
        resp = {}
        resp[:message_body] = message_body
        resp[:properties] = props unless opts[:properties].nil?
        resp[:envelope] = response.envelope unless opts[:envelope].nil?
        resp
      else
        message_body
      end
    end

    def subscribe(&block)
      auto_ack = false
      @channel.basic_consume(@name, auto_ack, QueueConsumer.new(@channel, block))
    end

    def loop_subscribe(&block)
      auto_ack = false
      consumer = QueueingConsumer.new(@channel)
      @channel.basic_consume(@name, auto_ack, consumer)
      loop do
        begin
          delivery = consumer.next_delivery
          message_body = String.from_java_bytes(delivery.get_body)
          case block.arity
          when 1
            block.call message_body
          when 2
            properties = delivery.get_properties
            block.call message_body, properties
          when 3
            properties = delivery.get_properties
            envelope = delivery.get_envelope
            block.call message_body, properties, envelope
          end
          @channel.basic_ack(delivery.get_envelope.get_delivery_tag, false)
        rescue InterruptedException => ie
          #next
        end
      end
    end

    def reactive_loop_subscribe(opts={}, &block) #block take 1 arg, a ReactiveMessage
      auto_ack = opts[:auto_ack] || false
      consumer = QueueingConsumer.new(@channel)
      @channel.basic_consume(@name, auto_ack, consumer)
      loop do
        begin
          delivery = consumer.next_delivery
          message_body = String.from_java_bytes(delivery.get_body)
          remsg = ReactiveMessage.new(@channel, delivery, message_body)
          block.call remsg
          @channel.basic_ack(delivery.envelope.delivery_tag, false) if remsg.should_acknowledge?
        rescue InterruptedException => ie
          #next
        end
        # next (above) is only necessary if you don't want any code here executed
      end
    end

    def purge
      @channel.queue_purge(@name)
    end

  end

  class ReactiveMessage
    attr_reader :body,:envelope,:properties
    def initialize(channel,delivery,body = nil)
      @channel,@delivery = channel,delivery
      @body = body || delivery.body
      @envelope = delivery.envelope
      @properties = delivery.properties
      @reacted = false
    end
    def ack!(multiple=false)
      @channel.basic_ack(@envelope.delivery_tag, multiple)
      @reacted = true
    end
    def reject!(requeue = true)
      @channel.basic_reject(@envelope.delivery_tag, requeue)
      @reacted = true
    end
    def should_acknowledge?
      !@reacted
    end
  end

  class Exchange
    attr_reader :name, :durable

    def initialize(name, type, channel, opts={})
      @name = name
      @type = type
      @channel = channel

      @durable, @auto_delete,@args = opts.values_at(:durable,:auto_delete,:args)
      # Declare a non-passive, auto-delete exchange
      @channel.exchange_declare(@name, type.to_s, durable, @auto_delete || false, @args)
      self
    end

    def match_opts(opts)
      dur,aut,arg = opts.values_at(:durable,:auto_delete,:args)
      @durable == dur && @auto_delete == aut && @args == arg
    end

#   PUBLISH_OPTS = {
#     :immediate => false,
#     :mandatory => true,
#     :persistent => true,
#     :routing_key => 'messages.xyz'
#   }
#
# @option options [String] :routing_key (nil)  Specifies message routing key. Routing key determines
#                                      what queues messages are delivered to (exact routing algorithms vary
#                                      between exchange types).
#
# @option options [Boolean] :mandatory (false) This flag tells the server how to react if the message cannot be
#                                      routed to a queue. If message is mandatory, the server will return
#                                      unroutable message back to the client with basic.return AMQPmethod.
#                                      If message is not mandatory, the server silently drops the message.
#
# @option options [Boolean] :immediate (false) This flag tells the server how to react if the message cannot be
#                                      routed to a queue consumer immediately.  If this flag is set, the
#                                      server will return an undeliverable message with a Return method.
#                                      If this flag is zero, the server will queue the message, but with
#                                      no guarantee that it will ever be consumed.
#
# @option options [Boolean] :persistent (false) When true, this message will be persisted to disk and remain in the queue until
#                                       it is consumed. When false, the message is only kept in a transient store
#                                       and will lost in case of server restart.
#                                       When performance and latency are more important than durability, set :persistent => false.
#                                       If durability is more important, set :persistent => true.
#
# @option options [String] :content_type (application/octet-stream) Content-type of message payload.
    def publish(msg, opts)
      routing_key = opts[:routing_key] || opts[:key]
      mandatory =   opts[:mandatory]  || false
      immediate =   opts[:immediate]  || false
      persistent =  opts[:persistent] || false
      headers =     opts[:headers]
      _type =       opts[:type] || 'basic'

      properties = case _type
      when 'minimal'
        if persistent
          RabbitMQClient::MessageProperties::MINIMAL_PERSISTENT_BASIC
        else
          RabbitMQClient::MessageProperties::MINIMAL_BASIC
        end
      when 'basic'
        if persistent
          RabbitMQClient::MessageProperties::PERSISTENT_BASIC
        else
          RabbitMQClient::MessageProperties::BASIC
        end
      else
        if persistent
          RabbitMQClient::MessageProperties::PERSISTENT_TEXT_PLAIN
        else
          RabbitMQClient::MessageProperties::TEXT_PLAIN
        end
      end

      properties.headers = headers if headers

      returns, confirms = opts.values_at(:listen_for_returns,:listen_for_confirms)
      time_out = opts[:time_out] || 0.01

      if returns || confirms
        feedback_queue = SizedQueue.new(1)
        @channel.return_listener = ReturnedMessageListener.new(lambda {|reply| feedback_queue << reply}) if returns
        @channel.confirm_listener = ConfirmedMessageListener.new(lambda {|reply| feedback_queue << reply}) if confirms
      end

      internal_publish msg, routing_key, properties, mandatory, immediate

      if returns || confirms
        Thread.new(time_out) do |n|
          sleep n
          feedback_queue << {:kind => "TIME_OUT"}
        end
        ret = feedback_queue.pop
        @channel.return_listener = nil if returns
        @channel.confirm_listener = nil if confirms
        ret
      end
    end

    private
    # Set props for different type of message. Currently they are:
    # RabbitMQClient::MessageProperties::MINIMAL_BASIC
    # RabbitMQClient::MessageProperties::MINIMAL_PERSISTENT_BASIC
    # RabbitMQClient::MessageProperties::BASIC
    # RabbitMQClient::MessageProperties::PERSISTENT_BASIC
    # RabbitMQClient::MessageProperties::TEXT_PLAIN
    # RabbitMQClient::MessageProperties::PERSISTENT_TEXT_PLAIN
    def internal_publish(message_body, routing_key='', props=nil, mandatory=false, immediate=false)
      raise RabbitMQClientError, "message cannot be converted to java bytes for publishing" unless message_body.respond_to?(:to_java_bytes)
      raise RabbitMQClientError, "properties cannot be nil" if props.nil?
      if props.kind_of? Hash
        properties = Java::ComRabbitmqClient::AMQP::BasicProperties.new()
        props.each {|k,v| properties.send(:"#{k.to_s}=", v) }
      else
        properties = props
      end
      @channel.basic_publish(@name, routing_key, mandatory, immediate, properties, message_body.to_java_bytes)
    end

  end

  # Class Methods
  class << self
  end

  attr_reader :channel
  attr_reader :connection

  # Instance Methods
  def initialize(options={})
    # server address
    @host = options[:host] || '127.0.0.1'
    @port = options[:port] || 5672

    # login details
    @username = options[:user] || 'guest'
    @password = options[:pass] || 'guest'
    @vhost = options[:vhost] || '/'

    # queues and exchanges
    @queues = {}
    @exchanges = {}

    connect unless options[:no_auto_connect]
    # Disconnect before the object is destroyed
    define_finalizer(self, lambda {|id| self.disconnect if self.connected? })
    self
  end

  def connect
    conn_factory = ConnectionFactory.new
    conn_factory.username = @username
    conn_factory.password = @password
    conn_factory.virtual_host = @vhost
    conn_factory.requested_heartbeat = 0
    conn_factory.host = @host
    conn_factory.port = @port
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

  def find_queue name
    @queues[name]
  end

  def queue name, opts
    @queues[name] = Queue.new(name, @channel, opts)
  end

  def find_exchange name
    @exchanges[name]
  end

  def exchange name, kind='fanout', opts={}
    @exchanges[name] = Exchange.new(name, kind, @channel, opts)
  end

  def self.delete_queue name
    @channel.queue_delete name
    @queues.delete name
  end

  def self.delete_exchange name
    @channel.exchange_delete name
    @exchanges.delete name
  end

  def qos(prefetch_size = 0, prefetch_count = 32, global = false)
    @channel.basic_qos(prefetch_size, prefetch_count, global)
  end
end

