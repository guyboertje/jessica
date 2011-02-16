require 'rubygems'
require 'spec'
require File.dirname(__FILE__) + '/../lib/rabbitmq_client'
require File.dirname(__FILE__) + '/../lib/rabbitmq_queue'
require File.dirname(__FILE__) + '/../lib/rabbitmq_rpc_server'
require File.dirname(__FILE__) + '/../lib/rabbitmq_rpc_client'

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
    @rmqclient = RabbitMQClient.new
  end
  
  after(:each) do
    @rmqclient.disconnect
  end
  
  describe RabbitMQClient::RabbitMQRpcServer do
    after(:each) do
      @rmqclient.delete_rpc_server(@svr.queue_name)
    end

    it "should create a basic rpc server with a random queue name" do
      @svr = @rmqclient.rpc_server
      @svr.should_not == nil
      @svr.queue_name.should =~ /^amq\.gen/
    end
    
    it "should create a basic rpc server with given queue name" do
      @svr = @rmqclient.rpc_server("test_rpc_server")
      @svr.should_not == nil
      @svr.queue_name.should == "test_rpc_server"
    end
    
    it "should create the queue if it does not exist" do
      @svr = @rmqclient.rpc_server("test_rpc_server")
      @svr.should_not == nil
      @svr.queue_name.should == "test_rpc_server"
    end
    
    it "should create an exchange to match the auto generated queue name" do
      @svr = @rmqclient.rpc_server()
      lambda { @rmqclient.channel.exchange_declare_passive(@svr.exchange) }.should_not raise_error(java.io.IOException)
    end
    
    it "should create an exchange to match a given queue name" do
      @svr = @rmqclient.rpc_server("test_rpc_server")
      lambda { @rmqclient.channel.exchange_declare_passive("test_rpc_server") }.should_not raise_error(java.io.IOException)
    end
    
    it "it should be able to bind an auto generated queue to an existing exchange" do
      @rmqclient.exchange('test_rpc_exchange', 'direct', false, true)
      @svr = @rmqclient.rpc_server(nil, "test_rpc_exchange")
      @svr.queue_name.should =~ /^amq\.gen/
      lambda { @rmqclient.channel.exchange_declare_passive("test_rpc_exchange") }.should_not raise_error(java.io.IOException)
    end
    
  end
  
  describe RabbitMQClient::RabbitMQRpcServer do
    before(:each) do
      @rmqclient.exchange('test_rpc_exchange', 'direct', false, true)
      @svr = @rmqclient.rpc_server("test_rpc_server", 'test_rpc_exchange', '', false) { |m, p| m }
    end
    after(:each) do
      exchange = @svr.exchange
      @rmqclient.delete_rpc_server(@svr.queue_name)
    end
    
    describe RabbitMQClient::RabbitMQRpcClient do
      before(:each) do
        @client1 = @rmqclient.rpc_client("test1", "test_rpc_exchange", '')
        @client2 = @rmqclient.rpc_client("test2", "test_rpc_exchange", '')
      end
      after(:each) do
        @rmqclient.delete_rpc_client("test1")
        @rmqclient.delete_rpc_client("test2")
      end
    
      it "should receive it's message back" do
        Thread.new do
          @ret1 = @client1.call("hello")
        end
        Thread.new do
          @ret2 = @client2.call("goodbye")
        end
        sleep 1
        @ret1.should == "hello"      
        @ret2.should == "goodbye"      
      end
    end
  end
end