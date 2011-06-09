#attempt to create an api that is compatible with the amqp gem
require 'rabbitmq_client'

module AMQP
  IncompatibleOptionsError = Class.new(StandardError)
  ClientConnectorError = Class.new(StandardError)
  def self.settings
    @settings ||= AMQP::Client::Settings.default
  end
  module Client
    module Settings
      def self.default
        @default ||= {
          # server
          :host  => "127.0.0.1", :port  => 5672,
          # login
          :user  => "guest", :pass  => "guest", :vhost => "/",
          # connection timeout
          :timeout => nil,
          # logging
          :logging => false,
          # ssl
          :ssl => false
        }
      end
      def self.configure(settings = nil)
        case settings
        when Hash then
          if user = settings.delete(:username)
            settings[:user] ||= user
          end
          if pass = settings.delete(:password)
            settings[:pass] ||= pass
          end
          self.default.merge(settings)
        when NilClass then
          self.default
        end
      end
    end
  end
  class Connection
    def self.new(options=nil)
      @options = AMQP::Client::Settings.configure(options || AMQP.settings)
      RabbitMQClient.new(@options)
    end
  end
  class Channel
    attr_reader :connection,:options
    ##
    #@host = options[:host] || '127.0.0.1'
    #@port = options[:port] || 5672
    #@username = options[:username] || 'guest'
    #@password = options[:password] || 'guest'
    #@vhost = options[:vhost] || '/'

    def initialize(connection=nil,channel_id=1,opts={})
      @options = opts
      @connection = connection || AMQP::Connection.new(@options)
    end

    def channel_number
      @connection.channel.channel_number
    end

    def connected?
      @connection.connected?
    end

    def connect
      return if connected?
      @connection.connect
    end

    def disconnect
      @connection.disconnect
    end

    def direct(name = 'amq.direct', opts = {}, &block)
      if exchange = @connection.find_exchange(name)
        raise AMQP::IncompatibleOptionsError.new() unless exchange.match_opts(opts)
      else
        exchange = @connection.exchange(name, 'direct', opts)
      end
      block.call(exchange) if block
      exchange
    end

    def fanout(name = 'amq.fanout', opts = {}, &block)
      if exchange = @connection.find_exchange(name)
        raise AMQP::IncompatibleOptionsError.new() unless exchange.match_opts(opts)
      else
        exchange = @connection.exchange(name, 'fanout', opts)
      end
      block.call(exchange) if block
      exchange
    end

    def headers(name = 'amq.headers', opts = {}, &block)
      if exchange = @connection.find_exchange(name)
        raise AMQP::IncompatibleOptionsError.new() unless exchange.match_opts(opts)
      else
        exchange = @connection.exchange(name, 'headers', opts)
      end
      block.call(exchange) if block
      exchange
    end

    def topic(name = 'amq.topic', opts = {}, &block)
      if exchange = @connection.find_exchange(name)
        raise AMQP::IncompatibleOptionsError.new() unless exchange.match_opts(opts)
      else
        exchange = @connection.exchange(name, 'topic', opts)
      end
      block.call(exchange) if block
      exchange
    end

    def queue(name=nil, opts={}, &block)
      raise ArgumentError.new("queue name must not be nil") if name.nil?
      raise ArgumentError.new("queue name must not be empty") if name.empty?
      if queue = @connection.find_queue(name)
        raise AMQP::IncompatibleOptionsError.new() unless queue.match_opts(opts)
      else
        queue = @connection.queue(name, opts)
      end
      block.call(queue) if block
      queue
    end

    private

  end
end
