require File.dirname(__FILE__) + '/rabbitmq_client.rb'
require 'uuid'

class RabbitMQClient
  
  class RabbitMQRpcClient
    include_class('com.rabbitmq.client.RpcClient')
    attr_reader :exchange, :routing_key, :name, :marshaller

    def initialize(channel, name, exchange, routing_key, marshaller=false)
      @marshaller = (marshaller == false) ? DefaultMarshaller : marshaller
      @name = name
      @channel = channel
      @exchange = exchange
      @routing_key = routing_key || @name
      @client = RpcClient.new(@channel, @exchange, @routing_key)
    end
    
    def call(message_body, props=nil)
      body = @marshaller.nil? ? message_body : @marshaller.dump(message_body)
      case
      when props.nil?
        properties = RabbitMQClient::MessageProperties::TEXT_PLAIN.clone
      when props.kind_of?(Hash)
        properties = RabbitMQClient::MessageProperties::TEXT_PLAIN.clone
        ["contentType", "contentEncoding", "deliveryMode", "priority", "userId", "appId"].each { |k| properties.send(:"#{k}=", props.send(:"#{k}")) }
      else
        properties = props
      end
      ret = @client.primitive_call(properties, body)
      @marshaller.nil? ? ret : @marshaller.load(ret)
    end
    
    def marshaller=(marshaller)
      @marshaller = (marshaller == false) ? DefaultMarshaller : marshaller
    end
    
    def queue_name
      @client.getReplyQueue
    end
  
    def close
      @client.close
    end
  end
end