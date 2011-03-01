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
    @rmqclient = RabbitMQClient.new(:marshaller=>false)
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
    
    it "should be able to handle calls with a 1-arity block" do
      @rmqclient.exchange('test_rpc_exchange', 'direct', false, true)
      @svr = @rmqclient.rpc_server("test_rpc_server", 'test_rpc_exchange', '', false) { |m| m }
      @client1 = @rmqclient.rpc_client("test1", "test_rpc_exchange", '', false)
      Thread.new do
        @ret1 = @client1.call("hello".to_java_bytes)
      end
      sleep 0.1
      String.from_java_bytes(@ret1).should == "hello"
      @rmqclient.delete_rpc_client("test1")
      @rmqclient.delete_rpc_server(@svr.queue_name)
    end
    
    it "should be able to handle calls with a 2-arity block" do
      @rmqclient.exchange('test_rpc_exchange', 'direct', false, true)
      @svr = @rmqclient.rpc_server("test_rpc_server", 'test_rpc_exchange', '', false) { |m, p| p.class.to_s.to_java_bytes }
      @client1 = @rmqclient.rpc_client("test1", "test_rpc_exchange", '', false)
      Thread.new do
        @ret1 = @client1.call("hello".to_java_bytes)
      end
      sleep 0.1
      String.from_java_bytes(@ret1).should =~ /::BasicProperties/
      @rmqclient.delete_rpc_client("test1")
      @rmqclient.delete_rpc_server(@svr.queue_name)
    end
    
    it "should be able to handle calls with a 3-arity block" do
      @rmqclient.exchange('test_rpc_exchange', 'direct', false, true)
      @svr = @rmqclient.rpc_server("test_rpc_server", 'test_rpc_exchange', '', false) { |m, p, r| r.class.to_s.to_java_bytes }
      @client1 = @rmqclient.rpc_client("test1", "test_rpc_exchange", '', false)
      Thread.new do
        @ret1 = @client1.call("hello".to_java_bytes)
      end
      sleep 0.1
      String.from_java_bytes(@ret1).should =~ /::BasicProperties/
      @rmqclient.delete_rpc_client("test1")
      @rmqclient.delete_rpc_server(@svr.queue_name)
    end
    
    it "should be able to handle calls with a 4-arity block" do
      @rmqclient.exchange('test_rpc_exchange', 'direct', false, true)
      @svr = @rmqclient.rpc_server("test_rpc_server", 'test_rpc_exchange', '', false) { |m, p, r, e| e.class.to_s.to_java_bytes }
      @client1 = @rmqclient.rpc_client("test1", "test_rpc_exchange", '', false)
      Thread.new do
        @ret1 = @client1.call("hello".to_java_bytes)
      end
      sleep 0.1
      String.from_java_bytes(@ret1).should =~ /::Envelope/
      @rmqclient.delete_rpc_client("test1")
      @rmqclient.delete_rpc_server(@svr.queue_name)
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
        @client1 = @rmqclient.rpc_client("test1", "test_rpc_exchange", '', false)
        @client2 = @rmqclient.rpc_client("test2", "test_rpc_exchange", '', false)
      end
      after(:each) do
        @rmqclient.delete_rpc_client("test1")
        @rmqclient.delete_rpc_client("test2")
      end
    
      it "should receive the correct message back" do
        Thread.new do
          @ret1 = @client1.call("hello".to_java_bytes)
        end
        Thread.new do
          @ret2 = @client2.call("goodbye".to_java_bytes)
        end
        sleep 0.1
        String.from_java_bytes(@ret1).should == "hello"      
        String.from_java_bytes(@ret2).should == "goodbye"      
      end
    end
  end
end