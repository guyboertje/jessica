require 'rubygems'
require 'spec'
require File.dirname(__FILE__) + '/../lib/rabbitmq_client'
require File.dirname(__FILE__) + '/../lib/rabbitmq_queue'

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
    @client.marshaller.should == RabbitMQClient::DefaultMarshaller
    lambda { @client.marshaller = MyMarshaller }.should_not raise_error(RabbitMQClient::RabbitMQClientError)
    @client.marshaller.should == MyMarshaller
  end

end