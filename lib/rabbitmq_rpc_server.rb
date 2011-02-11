require File.dirname(__FILE__) + '/rabbitmq_client.rb'

class RabbitMQClient
  include_class('com.rabbitmq.client.RpcServer')
  
  class RabbitMQRpcServer < RpcServer
    attr_reader :queue_name, :marshaller, :queue_name
    def initialize(channel, name, marshaller, proc=nil)
      @marshaller = RabbitMQClient.select_marshaller(marshaller)
      @channel = channel
      if name.nil?
        super(channel)
      else
        Queue.new(name, @channel, false, @marshaller)
        super(channel, name)
      end
      @queue_name = self.get_queue_name
      @block = block if block_given?
    end
    
    def marshaller=(marshaller)
      @marshaller = RabbitMQClient.select_marshaller(marshaller)
    end
  
    def set_handler(meth)
      method(:meth).arity
    end
    
    def setup_consumer
      setupConsumer
    end
    
    def handleCall(request_properties, request_body, reply_properties)
      java_signature 'byte[] handleCall(AMQP.BasicProperties request_properties, byte[] request_body, AMQP.BasicProperties reply_properties)'
      case @block.arity?
      when 1
        print 'block arity 1'
        ret = @block.call(request_body)
      when 2
        print 'block arity 2'
        ret = @block.call(request_body, reply_properties)
      when 3
        print 'block arity 3'
        ret = @block.call(request_body, reply_properties, request_properties)
      end
    end
  end
end