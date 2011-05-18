require 'rubygems'
require 'spec'
require File.dirname(__FILE__) + '/../lib/rabbitmq_client'

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
  
  it "should raise an exception for a broken marshaller" do
    lambda { @client = RabbitMQClient.new({:no_auto_correct=>true, :marshaller=>BrokenMarshaller}) }.should raise_error(RabbitMQClient::RabbitMQClientError)
  end
  
  it "should allow no marshaller" do
    lambda { @client = RabbitMQClient.new({:no_auto_correct=>true, :marshaller=>false}) }.should_not raise_error(RabbitMQClient::RabbitMQClientError)
  end
  
end


describe RabbitMQClient do
  before(:each) do
    @client = RabbitMQClient.new
  end
  
  after(:each) do
    @client.disconnect
  end
  
  it "should able to create a connection" do
    @client.connection.should_not be_nil
  end
  
  it "should able to create a channel" do
    @client.channel.should_not be_nil
  end
  
  it "should be able to create a new exchange" do
    exchange = @client.exchange('test_exchange', 'direct')
    exchange.should_not be_nil
  end
  
  it "should raise an exception creating a queue with a broken marshaller" do
    lambda { @client.queue('test_queue', false, BrokenMarshaller) }.should raise_error(RabbitMQClient::RabbitMQClientError)
  end

  it "should raise an exception setting a broken marshaller using the accessor" do
    lambda { @client.marshaller = BrokenMarshaller }.should raise_error(RabbitMQClient::RabbitMQClientError)
  end
  
  it "should be able to set a marshaller using the accessor" do
    lambda { @client.marshaller = MyMarshaller }.should_not raise_error(RabbitMQClient::RabbitMQClientError)
  end
  
  describe Queue, "Basic non-persistent queue" do
    before(:each) do
      @queue = @client.queue('test_queue')
      @exchange = @client.exchange('test_exchange', 'direct')
    end
    
    after(:each) do
      @queue.purge
    end
    
    it "should be able to create a queue" do
      @queue.should_not be_nil
    end
    
    it "should be able to bind to an exchange" do
      @queue.bind(@exchange).should_not be_nil
    end
    
    it "should be able to publish and retrieve a message" do
      @queue.bind(@exchange)
      @queue.publish('Hello World')
      @queue.retrieve.should == 'Hello World'
      @queue.publish('人大')
      @queue.retrieve.should == '人大'
    end
    
    it "should be able to purge the queue" do
      @queue.publish('Hello World')
      @queue.publish('Hello World')
      @queue.publish('Hello World')
      @queue.purge
      @queue.retrieve.should == nil
    end
    
    it "should able to subscribe with a callback function" do
      a = 0
      @queue.bind(@exchange)
      @queue.subscribe do |v|
         a += v.to_i
      end
      @queue.publish("1")
      @queue.publish("2")
      sleep 1
      a.should == 3
    end

    it "should able to subscribe with a 2-arity callback function" do
      a = []
      @queue.bind(@exchange)
      @queue.subscribe do |v, p|
         a << p.nil?
      end
      @queue.publish("1")
      @queue.publish("2")
      sleep 1
      a.should == [false,false]
    end

    it "should able to subscribe with a 3-arity callback function" do
      a = []
      @queue.bind(@exchange)
      @queue.subscribe do |v, p, e|
         a << e.nil?
      end
      @queue.publish("1")
      @queue.publish("2")
      sleep 1
      a.should == [false,false]
    end

    it "should be able to subscribe to a queue using loop_subscribe" do
      a = 0
      @queue.bind(@exchange)
      Thread.new do
        begin
          timeout(1) do
            @queue.loop_subscribe do |v|
              a += v.to_i
            end
          end
        rescue Timeout::Error => e
        end
      end
      @queue.publish("1")
      @queue.publish("2")
      sleep 1
      a.should == 3
    end

    it "should be able to subscribe to a queue using loop_subscribe with a 2-arity callback" do
      a = []
      @queue.bind(@exchange)
      Thread.new do
        begin
          timeout(1) do
            @queue.loop_subscribe do |v, p|
              a << p.nil?
            end
          end
        rescue Timeout::Error => e
        end
      end
      @queue.publish("1")
      @queue.publish("2")
      sleep 1
      a.should == [false,false]
    end

    it "should be able to subscribe to a queue using loop_subscribe with a 3-arity callback" do
      a = []
      @queue.bind(@exchange)
      Thread.new do
        begin
          timeout(1) do
            @queue.loop_subscribe do |v, p, e|
              a << e.nil?
            end
          end
        rescue Timeout::Error => e
        end
      end
      @queue.publish("1")
      @queue.publish("2")
      sleep 1
      a.should == [false,false]
    end

    it "should be able to subscribe to a queue using reactive_loop_subscribe with a implicit ack" do
      a = []
      @queue.bind(@exchange)
      Thread.new do
        begin
          timeout(1) do
            @queue.reactive_loop_subscribe do |msg|
              a << msg.body
              #msg.ack!
            end
          end
        rescue Timeout::Error => e
        end
      end
      @queue.publish("1")
      @queue.publish("2")
      sleep 1
      a.should == ["1","2"]
    end
    
    it "should raise an exception if binding a persistent queue with a non-persistent exchange and vice versa" do
      persistent_queue = @client.queue('test_queue1', true)
      persistent_exchange = @client.exchange('test_exchange1', 'fanout', true)
      lambda { persistent_queue.bind(@exchange) }.should raise_error(RabbitMQClient::RabbitMQClientError)
      lambda { @queue.bind(persistent_exchange) }.should raise_error(RabbitMQClient::RabbitMQClientError)
      persistent_queue.delete
      persistent_exchange.delete
    end
    
    it "should raise an exception if publish a persistent message on non-durable queue" do
      @queue.bind(@exchange)
      lambda { @queue.persistent_publish('Hello') }.should raise_error(RabbitMQClient::RabbitMQClientError)
    end
    
    it "should raise an exception if the marshaller is changed to an invalid class" do
      @queue.bind(@exchange)
      lambda { @queue.marshaller = BrokenMarshaller }.should raise_error(RabbitMQClient::RabbitMQClientError)
    end

    it "should be able to set a valid marshaller using the accessor" do
      @queue.marshaller.should == RabbitMQClient::DefaultMarshaller
      @queue.bind(@exchange)
      lambda { @queue.marshaller = MyMarshaller }.should_not raise_error(RabbitMQClient::RabbitMQClientError)
      @queue.marshaller.should == MyMarshaller
    end
    
    it "should be able to get the message envelope through retrieve" do
      @queue.bind(@exchange)
      @queue.publish('Hello World')
      m = @queue.retrieve(:envelope=>true)
      m[:message_body].should == 'Hello World'
      m[:envelope].class.should == Java::ComRabbitmqClient::Envelope
    end

    it "should be able to get the message properties through retrieve" do
      @queue.bind(@exchange)
      @queue.publish('Hello World')
      m = @queue.retrieve(:properties=>true)
      m[:message_body].should == 'Hello World'
      m[:properties].class.should == Java::ComRabbitmqClient::AMQP::BasicProperties
    end
    
    it "should be able to set message properties through a hash passed to publish" do
      @queue.bind(@exchange)
      @queue.publish('Hello World', :priority=>1, :content_type=>'text/plain')
      m = @queue.retrieve(:properties=>true)
      m[:message_body].should == 'Hello World'
      m[:properties].priority.should == 1
    end

    it "should be able to set message properties through a BasicProperties object passed to publish" do
      @queue.bind(@exchange)
      props = RabbitMQClient::MessageProperties::TEXT_PLAIN
      props.priority = 1
      @queue.publish('Hello World', props)
      m = @queue.retrieve(:properties=>true)
      m[:message_body].should == 'Hello World'
      m[:properties].priority.should == 1
    end
  end
  
  describe Queue, "Basic persistent queue" do
    before(:each) do
      @queue = @client.queue('test_durable_queue', true)
      @exchange = @client.exchange('test_durable_exchange', 'fanout', true)
    end
    
    after(:each) do
      @queue.purge
    end
    
    it "should be able to create a queue" do
      @queue.should_not be_nil
    end
    
    it "should be able to bind to an exchange" do
      @queue.bind(@exchange).should_not be_nil
    end
    
    it "should be able to publish and retrieve a message" do
      @queue.bind(@exchange)
      @queue.persistent_publish('Hello World')
      @queue.retrieve.should == 'Hello World'
    end
    
    it "should be able to subscribe to a queue using reactive_loop_subscribe with an explicit reject" do
      a = []
      @queue.bind(@exchange)
      Thread.new do
        begin
          timeout(0.25) do
            @queue.reactive_loop_subscribe do |msg|
              print '+'
              msg.reject!
            end
          end
        rescue Timeout::Error => e
        end
      end
      @queue.persistent_publish("1")
      sleep 0.25
      a.should == []
    end
    
    #should get the previously rejected message as well
    it "should be able to subscribe with a callback function" do
      a = 0
      @queue.bind(@exchange)
      @queue.subscribe do |v|
         a += v.to_i
      end
      @queue.persistent_publish("1")
      @queue.persistent_publish("2")
      sleep 1
      a.should == 4
    end
  end

  describe Queue, "Basic, non-marshalled queue" do
    before(:each) do
      @queue = @client.queue('test_queue', false, nil)
      @exchange = @client.exchange('test_exchange', 'direct')
    end

    after(:each) do
      @queue.purge
    end
    
    it "should be able to publish and retrieve a message" do
      @queue.bind(@exchange)
      @queue.publish(Marshal.dump('Hello World').to_java_bytes)
      Marshal.load(String.from_java_bytes(@queue.retrieve)).should == 'Hello World'
      @queue.publish(Marshal.dump('人大').to_java_bytes)
      Marshal.load(String.from_java_bytes(@queue.retrieve)).should == '人大'
    end

    it "should be able to purge the queue" do
      @queue.publish(Marshal.dump('Hello World').to_java_bytes)
      @queue.publish(Marshal.dump('Hello World').to_java_bytes)
      @queue.publish(Marshal.dump('Hello World').to_java_bytes)
      @queue.purge
      @queue.retrieve.should == nil
    end

    it "should able to subscribe with a callback function" do
      a = 0
      @queue.bind(@exchange)
      @queue.subscribe do |v|
         a += Marshal.load(String.from_java_bytes(v)).to_i
      end
      @queue.publish(Marshal.dump('1').to_java_bytes)
      @queue.publish(Marshal.dump('2').to_java_bytes)
      sleep 1
      a.should == 3
    end

    it "should be able to subscribe to a queue using loop_subscribe" do
      a = 0
      @queue.bind(@exchange)
      Thread.new do
        begin
          timeout(1) do
            @queue.loop_subscribe do |v|
              a += Marshal.load(String.from_java_bytes(v)).to_i
            end
          end
        rescue Timeout::Error => e
        end
      end
      @queue.publish(Marshal.dump('1').to_java_bytes)
      @queue.publish(Marshal.dump('2').to_java_bytes)
      sleep 1
      a.should == 3
    end

    it "should raise an exception if trying to publish an object that is not marshalled to java bytes" do
      @queue.bind(@exchange)
      lambda { @queue.publish(12345) }.should raise_error(RabbitMQClient::RabbitMQClientError)
    end
  end

  describe Queue, "Basic, client-defined marshalling queue" do
    before(:each) do
      @queue = @client.queue('test_queue', false, MyMarshaller)
      @exchange = @client.exchange('test_exchange', 'direct')
    end

    after(:each) do
      @queue.purge
    end
    
    it "should be able to publish and retrieve a message" do
      @queue.bind(@exchange)
      @queue.publish('Hello World')
      @queue.retrieve.should == 'Hello World'
      @queue.publish('人大')
      @queue.retrieve.should == '人大'
    end

    it "should be able to purge the queue" do
      @queue.publish('Hello World')
      @queue.publish('Hello World')
      @queue.publish('Hello World')
      @queue.purge
      @queue.retrieve.should == nil
    end

    it "should able to subscribe with a callback function" do
      a = 0
      @queue.bind(@exchange)
      @queue.subscribe do |v|
         a += v.to_i
      end
      @queue.publish('1')
      @queue.publish('2')
      sleep 1
      a.should == 3
    end

    it "should be able to subscribe to a queue using loop_subscribe" do
      a = 0
      @queue.bind(@exchange)
      Thread.new do
        begin
          timeout(1) do
            @queue.loop_subscribe do |v|
              a += v.to_i
            end
          end
        rescue Timeout::Error => e
        end
      end
      @queue.publish("1")
      @queue.publish("2")
      sleep 1
      a.should == 3
    end
  end

end
