#! /usr/bin/env jruby

spec = Gem::Specification.new do |s|
  s.name = 'rabbitmq-jruby-client'
  s.version = '0.9.1'
  s.authors = ['Guy Boertje','Jerry Luk']
  s.email = 'jerryluk@gmail.com'
  s.date = '2009-02-09'
  s.summary = 'A RabbitMQ client for JRuby'
  s.description = s.summary + 'with some AMQP gem api compatibility'
  s.homepage = 'http://www.linkedin.com/in/jerryluk'
  s.require_path = 'lib'
  s.files = ["README", "MIT-LICENSE", "lib/commons-cli-1.1.jar", "lib/commons-io-1.2.jar", "lib/rabbitmq-client.jar",
              "lib/rabbitmq_client.rb", "spec/rabbitmq_client_spec.rb"]
  s.test_files = ["spec/rabbitmq_client_spec.rb"]
  s.has_rdoc = false
end
