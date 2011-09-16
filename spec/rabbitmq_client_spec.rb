# encoding: utf-8
$LOAD_PATH.unshift(File.dirname(__FILE__))

require 'ap'
require 'jessica'
require 'rspec'
require 'timeout'

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

  describe Queue, "Basic non-persistent queue with default marshaller" do
    before(:each) do
      @queue = @client.queue('test_queue', :durable => false, :auto_delete => false)
      @exchange = @client.exchange('test_exchange', 'direct', :durable => false, :default => true)
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

    it "should have the same marshaller as the exchange" do
      @queue.marshaller.class.should == @exchange.marshaller.class
    end

    it "should be able to publish and retrieve a message" do
      @queue.bind(@exchange)
      @exchange.publish('Hello World')
      @queue.retrieve.should == 'Hello World'
      @exchange.publish('人大')
      @queue.retrieve.should == '人大'
    end

    it "should be able to purge the queue" do
      @exchange.publish('Hello World')
      @exchange.publish('Hello World')
      @exchange.publish('Hello World')
      @queue.purge
      @queue.retrieve.should == nil
    end

    it "should able to subscribe with a callback function" do
      a = 0
      @queue.bind(@exchange)
      @queue.subscribe do |v|
         a += v.to_i
      end
      @exchange.publish("1")
      @exchange.publish("2")
      sleep 1
      a.should == 3
    end

    it "should able to subscribe with a 2-arity callback function" do
      a = []
      @queue.bind(@exchange)
      @queue.subscribe do |v, p|
         a << p.nil?
      end
      @exchange.publish("1")
      @exchange.publish("2")
      sleep 1
      a.should == [false,false]
    end

    it "should able to subscribe with a 3-arity callback function" do
      a = []
      @queue.bind(@exchange)
      @queue.subscribe do |v, p, e|
         a << e.nil?
      end
      @exchange.publish("1")
      @exchange.publish("2")
      sleep 1
      a.should == [false,false]
    end

    it "should be able to subscribe to a queue using loop_subscribe" do
      a = 0
      @queue.bind(@exchange)
      th = Thread.new do
        begin
          timeout(1) do
            @queue.loop_subscribe do |v|
              a += v.to_i
            end
          end
        rescue Timeout::Error => e
        end
      end
      th.abort_on_exception=true
      @exchange.publish("1")
      @exchange.publish("2")
      sleep 1
      a.should == 3
    end

    it "should be able to subscribe to a queue using loop_subscribe with a 2-arity callback" do
      a = []
      @queue.bind(@exchange)
      th = Thread.new do
        begin
          timeout(1) do
            @queue.loop_subscribe do |v, p|
              a << p.nil?
            end
          end
        rescue Timeout::Error => e
        end
      end
      th.abort_on_exception=true
      @exchange.publish("1")
      @exchange.publish("2")
      sleep 1
      a.should == [false,false]
    end

    it "should be able to subscribe to a queue using loop_subscribe with a 3-arity callback" do
      a = []
      @queue.bind(@exchange)
      th = Thread.new do
        begin
          timeout(1) do
            @queue.loop_subscribe do |v, p, e|
              a << e.nil?
            end
          end
        rescue Timeout::Error => e
        end
      end
      th.abort_on_exception=true
      @exchange.publish("1")
      @exchange.publish("2")
      sleep 1
      a.should == [false,false]
    end

    it "should be able to subscribe to a queue using reactive_loop_subscribe with a implicit ack" do
      a = []
      @queue.bind(@exchange)
      th = Thread.new do
        begin
          timeout(1) do
            @queue.reactive_loop_subscribe do |msg|
              a << msg.body
            end
          end
        rescue Timeout::Error => e
        end
      end
      th.abort_on_exception=true
      @exchange.publish("1")
      @exchange.publish("2")
      sleep 1
      a.should == ["1","2"]
    end

    it "should raise an exception if binding a persistent queue with a non-persistent exchange and vice versa" do
      persistent_queue = @client.queue('test_queue1', :durable => true)
      persistent_exchange = @client.exchange('test_exchange1', 'fanout', :durable => true)
      lambda { persistent_queue.bind(@exchange) }.should raise_error(RabbitMQClient::RabbitMQClientError)
      lambda { @queue.bind(persistent_exchange) }.should raise_error(RabbitMQClient::RabbitMQClientError)
      persistent_queue.delete
      persistent_exchange.delete
    end

    it "should raise an exception if publish a persistent message on non-durable queue" do
      @queue.bind(@exchange)
      lambda { @exchange.persistent_publish('Hello') }.should raise_error(RabbitMQClient::RabbitMQClientError)
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
      @exchange.publish('Hello World')
      m = @queue.retrieve(:envelope=>true)
      m[:message_body].should == 'Hello World'
      m[:envelope].class.should == Java::ComRabbitmqClient::Envelope
    end

    it "should be able to get the message properties through retrieve" do
      @queue.bind(@exchange)
      @exchange.publish('Hello World')
      m = @queue.retrieve(:properties=>true)
      m[:message_body].should == 'Hello World'
      m[:properties].class.should == Java::ComRabbitmqClient::AMQP::BasicProperties
    end

    it "should be able to set message properties through a hash passed to publish" do
      @queue.bind(@exchange)
      @exchange.publish('Hello World','', :priority=>1, :content_type=>'text/plain')
      m = @queue.retrieve(:properties=>true)
      m[:message_body].should == 'Hello World'
      m[:properties].priority.should == 1
    end

    it "should be able to set message properties through a BasicProperties object passed to publish" do
      @queue.bind(@exchange)
      props = RabbitMQClient::MessageProperties::TEXT_PLAIN
      props.priority = 1
      @exchange.publish('Hello World','', props)
      m = @queue.retrieve(:properties=>true)
      m[:message_body].should == 'Hello World'
      m[:properties].priority.should == 1
    end
  end

  describe Queue, "Basic persistent queue" do
    before(:each) do
      @queue = @client.queue('test_durable_queue', :durable => true)
      @exchange = @client.exchange('test_durable_exchange', 'fanout', :durable => true)
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
      @exchange.persistent_publish('Hello World')
      @queue.retrieve.should == 'Hello World'
    end

    it "should be able to subscribe to a queue using reactive_loop_subscribe with an explicit reject" do
      a = 0
      redeliver_flag = nil
      @queue.bind(@exchange)
      th = Thread.new do
        begin
          timeout(0.25) do
            @queue.reactive_loop_subscribe do |msg|
              a += 1
              if a > 1
                redeliver_flag = msg.envelope.redeliver?
                msg.ack!
              else
                msg.reject!
              end
            end
          end
        rescue Timeout::Error => e
        end
      end
      th.abort_on_exception=true
      @exchange.persistent_publish("1")
      sleep 0.25
      a.should > 1
      redeliver_flag.should be_true
    end

    it "should be able to subscribe with a callback function" do
      a = 0
      @queue.bind(@exchange)
      @queue.subscribe do |v|
         a += v.to_i
      end
      @exchange.persistent_publish("1")
      @exchange.persistent_publish("2")
      sleep 1
      a.should == 3
    end
  end

  describe Queue, "Basic, non-marshalled queue" do
    before(:each) do
      @queue = @client.queue('test_queue', :durable => false, :auto_delete => false)
      @exchange = @client.exchange('test_exchange', 'direct')
    end

    after(:each) do
      @queue.purge
    end

    it "should be able to publish and retrieve a message" do
      @queue.bind(@exchange)
      @exchange.publish(Marshal.dump('Hello World'))
      Marshal.load(@queue.retrieve).should == 'Hello World'
      @exchange.publish(Marshal.dump('人大'))
      Marshal.load(@queue.retrieve).should == '人大'
    end

    it "should be able to purge the queue" do
      @exchange.publish('Hello World')
      @exchange.publish('Hello World')
      @exchange.publish('Hello World')
      @queue.purge
      @queue.retrieve.should == nil
    end

    it "should able to subscribe with a callback function" do
      a = 0
      @queue.bind(@exchange)
      @queue.subscribe do |v|
         a += Marshal.load(v).to_i
      end
      @exchange.publish(Marshal.dump('1'))
      @exchange.publish(Marshal.dump('2'))
      sleep 1
      a.should == 3
    end

    it "should be able to subscribe to a queue using loop_subscribe" do
      a = 0
      @queue.bind(@exchange)
      th = Thread.new do
        begin
          timeout(1) do
            @queue.loop_subscribe do |v|
              a += Marshal.load(v).to_i
            end
          end
        rescue Timeout::Error => e
        end
      end
      th.abort_on_exception=true
      @exchange.publish(Marshal.dump('1'))
      @exchange.publish(Marshal.dump('2'))
      sleep 1
      a.should == 3
    end
  end

  describe Queue, "Basic, client-defined marshalling queue" do
    before(:each) do
      @queue = @client.queue('test_queue', :durable => false)
      @exchange = @client.exchange('test_exchange', 'direct', :durable => false)
    end

    after(:each) do
      @queue.purge
    end

    it "should be able to publish and retrieve a message" do
      @queue.bind(@exchange)
      @exchange.publish('Hello World')
      @queue.retrieve.should == 'Hello World'
      @exchange.publish('人大')
      @queue.retrieve.should == '人大'
    end

    it "should be able to purge the queue" do
      @exchange.publish('Hello World')
      @exchange.publish('Hello World')
      @exchange.publish('Hello World')
      @queue.purge
      @queue.retrieve.should == nil
    end

    it "should able to subscribe with a callback function" do
      a = 0
      @queue.bind(@exchange)
      @queue.subscribe do |v|
         a += v.to_i
      end
      @exchange.publish('1')
      @exchange.publish('2')
      sleep 1
      a.should == 3
    end

    it "should be able to subscribe to a queue using loop_subscribe" do
      a = 0
      @queue.bind(@exchange)
      th = Thread.new do
        begin
          timeout(1) do
            @queue.loop_subscribe do |v|
              a += v.to_i
            end
          end
        rescue Timeout::Error => e
        end
      end
      th.abort_on_exception=true
      @exchange.publish("1")
      @exchange.publish("2")
      sleep 1
      a.should == 3
    end
  end

  describe Queue, "Basic persistent queue with mandatory and immediate flags and return message handling" do
    before(:each) do
      @queue = @client.queue('test_durable_queue', :durable => true)
      @exchange = @client.exchange('test_direct_durable_exchange', 'direct', :durable => true)
      @publish_return = @publish_ack = nil
      @start = @stop = 0
      @client.register_callback(:return) {|ret| @publish_return = ret} #; @stop = java.lang.System.nano_time}
      @client.register_callback(:ack) {|ret| @publish_ack = ret}
      @queue.bind(@exchange,"black")
    end

    after(:each) do
      @queue.purge
    end

    it "should be able to publish and retrieve a message" do
      @exchange.persistent_publish('Hello World','black')
      @queue.retrieve.should == 'Hello World'
      @publish_return.should be_nil
      @publish_ack.should be_nil
    end

    it "should be able to publish mandatory with the wrong routing key and get a returned message" do
      #@start = java.lang.System.nano_time
      @exchange.persistent_publish('Hello World', 'red',  RabbitMQClient::MessageProperties::PERSISTENT_TEXT_PLAIN, true)
      sleep 0.2
      #puts "","time (us) to return a reject message: #{((@stop - @start)/1000).to_i}"
      @publish_return.class.should == Hash
      @publish_ack.should be_nil
    end

    it "should be able to publish mandatory with the right routing key and get an acked message and retrieve a message" do
      pending("addition of 'put channel into confirm mode' and 'add plumbing for tracking unconfirmed msgs'")
      @exchange.persistent_publish('Hello World', 'black',  RabbitMQClient::MessageProperties::PERSISTENT_TEXT_PLAIN, true)
      @queue.retrieve.should == 'Hello World'
      sleep 0.1
      puts "",@publish_ack.inspect
      @publish_return.should be_nil
      @publish_ack.class.should == Hash
    end
  end
end
