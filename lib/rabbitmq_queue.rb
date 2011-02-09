require File.dirname(__FILE__) + '/rabbitmq_client.rb'

class RabbitMQClient

  class Queue
    attr_reader :marshaller
  
    def initialize(name, channel, durable=false, marshaller=false)
      @name = name
      @durable = durable
      @channel = channel
      @marshaller = (marshaller == false) ? DefaultMarshaller : marshaller
      raise RabbitMQClientError, "invalid marshaller" unless @marshaller.nil? or (@marshaller.respond_to? :load and @marshaller.respond_to? :dump)
      exclusive = false
      auto_delete = false
      args = nil
      @channel.queue_declare(name, durable, exclusive, auto_delete, args)
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
  
    def bind(exchange, routing_key='')
      raise RabbitMQClientError, "queue and exchange have different durable properties" unless @durable == exchange.durable
      @routing_key = routing_key
      @exchange = exchange
      @channel.queue_bind(@name, @exchange.name, @routing_key)
      self
    end
  
    def unbind
      @channel.queue_unbind(@name, @exchange.name, @routing_key) if @exchange
    end
  
    # Set props for different type of message. Currently they are:
    # RabbitMQClient::MessageProperties::MINIMAL_BASIC
    # RabbitMQClient::MessageProperties::MINIMAL_PERSISTENT_BASIC
    # RabbitMQClient::MessageProperties::BASIC
    # RabbitMQClient::MessageProperties::PERSISTENT_BASIC
    # RabbitMQClient::MessageProperties::TEXT_PLAIN
    # RabbitMQClient::MessageProperties::PERSISTENT_TEXT_PLAIN
    def publish(message_body, props=RabbitMQClient::MessageProperties::TEXT_PLAIN)
      auto_bind
      message_body_byte = @marshaller.nil? ? message_body : @marshaller.send(:dump, message_body)
      unless message_body_byte.respond_to? :java_object and message_body_byte.java_object.class == Java::JavaArray
        raise RabbitMQClientError, "message not converted to java bytes for publishing"
      end
      if props.kind_of? Hash
        properties = Java::ComRabbitmqClient::AMQP::BasicProperties.new()
        props.each {|k,v| properties.send(:"#{k.to_s}=", v) }
      else
        properties = props
      end
      @channel.basic_publish(@exchange.name, @routing_key, properties, message_body_byte)
      message_body
    end
  
    def persistent_publish(message_body, props=MessageProperties::PERSISTENT_TEXT_PLAIN)
      raise RabbitMQClientError, "can only publish persistent message to durable queue" unless @durable
      publish(message_body, props)
    end
  
    def retrieve(opts={})
      auto_bind
      message_body = nil
      no_ack = opts[:no_ack] ? opts[:no_ack] : false
      response = @channel.basic_get(@name, no_ack)
      if response
        props = response.get_props
        message_body = @marshaller.nil? ? response.get_body : @marshaller.send(:load, response.get_body)
        delivery_tag = response.get_envelope.get_delivery_tag
        @channel.basic_ack(delivery_tag, false)
      end
      unless opts.empty?
        resp = {}
        resp[:message_body] = message_body
        resp[:properties] = props unless opts[:properties].nil?
        resp[:envelope] = response.get_envelope unless opts[:envelope].nil?
        resp
      else
        message_body
      end
    end
  
    def subscribe(&block)
      no_ack = false
      @channel.basic_consume(@name, no_ack, QueueConsumer.new(@channel, block, @marshaller))
    end
  
    def loop_subscribe(&block)
      no_ack = false
      consumer = QueueingConsumer.new(@channel)
      @channel.basic_consume(@name, no_ack, consumer)
      loop do
        begin
          delivery = consumer.next_delivery
          message_body = @marshaller.nil? ? delivery.get_body : @marshaller.send(:load, delivery.get_body)
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
          next
        end
      end
    end
  
    def purge
      @channel.queue_purge(@name)
    end
  
    def delete
      @channel.queue_delete(@name)
    end
  
    protected
    def auto_bind
      unless @exchange
        exchange = Exchange.new("#{@name}_exchange", 'fanout', @channel, @durable)
        self.bind(exchange)
      end
    end
  end

end
