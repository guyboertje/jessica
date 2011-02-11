require 'rubygems'
require 'spec'
require File.dirname(__FILE__) + '/../lib/rabbitmq_client'
require File.dirname(__FILE__) + '/../lib/rabbitmq_queue'
require File.dirname(__FILE__) + '/../lib/rabbitmq_rpc_server'

class BrokenMarshaller
  def self.dump(message)
    message.to_java_bytes
  end
end
class MyMarshaller
  def self.load(body)
    String.from_java_bytes(body)
  end
  def self.dump(message)
    message.to_java_bytes
  end
end

describe RabbitMQClient do
  before(:each) do
    @client = RabbitMQClient.new
  end
  
  after(:each) do
    @client.disconnect
  end
  
  describe RabbitMQClient::RabbitMQRpcServer do
    #after(:each) do
    #  @client.delete_rpc_server(@svr.queue_name)
    #end

    it "should create a basic rpc server with a random queue name" do
      @svr = @client.rpc_server
      @svr.should_not == nil
      @svr.queue_name.should =~ /^amq\.gen/
    end
    
    it "should create a basic rpc server with given queue name" do
      @svr = @client.rpc_server("test_rpc_server")
      @svr.should_not == nil
      @svr.queue_name.should == "test_rpc_server"
      @svr.channel.queue_delete(@svr.queue_name) rescue nil
    end
    
    it "should create the queue if it does no exist" do
      #@svr.channel.queue_delete(@svr.queue_name) rescue nil
      @svr = @client.rpc_server("test_rpc_server")
      @svr.should_not == nil
      @svr.queue_name.should == "test_rpc_server"
      @svr.channel.queue_delete(@svr.queue_name) rescue nil
    end
    
  end
end