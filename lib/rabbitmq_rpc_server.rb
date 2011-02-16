require File.dirname(__FILE__) + '/rabbitmq_client.rb'

class RabbitMQClient
  include_class('com.rabbitmq.client.RpcServer')
  
  class RabbitMQRpcServer < RpcServer
    attr_reader :queue_name, :marshaller, :exchange
    def initialize(channel, name=nil, exchange=nil, routing_key='', marshaller=false, proc=nil)
      @marshaller = (marshaller == false) ? DefaultMarshaller : marshaller
      @channel = channel
      if name.nil?
        super(channel)
      else
        @channel.queue_declare(name, false, false, true, nil)
        super(channel, name)
      end
      @queue_name = self.get_queue_name
      @routing_key = routing_key
      @exchange = exchange || @queue_name.gsub(/^amq\./, '')
      @channel.exchange_declare(@exchange, 'direct', false, true, nil) unless exchange
      @channel.queue_bind(@queue_name, @exchange, @routing_key)
      @block = block_given? ? block : proc
    end
    
    def start
      @thread = Thread.new { self.mainloop }
    end
    
    def marshaller=(marshaller)
      @marshaller = (marshaller == false) ? DefaultMarshaller : marshaller
    end

    def unbind_queue
      @channel.queue_unbind(@queue_name, @exchange, @routing_key)
    end

    def set_handler(meth)
      method(:meth).arity
    end
    
    def handleCall(request_properties, request_body, reply_properties)
      java_signature 'byte[] handleCall(AMQP.BasicProperties request_properties, byte[] request_body, AMQP.BasicProperties reply_properties)'
      body = @marshaller.nil? ? request_body : @marshaller.load(request_body)
      ["contentType", "contentEncoding", "deliveryMode", "priority", "correlationId", "replyTo"].each {|k| reply_properties.send(:"#{k}=", request_properties.send(:"#{k}"))}
      case @block.arity
      when 1
        ret = @block.call(body)
      when 2
        ret = @block.call(body, reply_properties)
      when 3
        ret = @block.call(body, reply_properties, request_properties)
      end
      ret
    end
  end
end