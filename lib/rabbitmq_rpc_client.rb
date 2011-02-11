require File.dirname(__FILE__) + '/rabbitmq_client.rb'

class RabbitMQClient
  include_class('com.rabbitmq.client.RpcClient')
  
  class RabbitMQRpcClient
    def initialize(channel, exchange, routing_key, marshaller=false)
      @marshaller = (marshaller == false) ? DefaultMarshaller : marshaller
      
      @server = RpcServer
    end
    
    def queue_name
      
    end
    
    def set_handler
      
    end
    
    def set_consumer
      
    end
    
    def close
      
    end
    
  end
end