Gem::Specification.new do |s|
  s.name        = 'jessica'
  s.version     = '1.0.2'
  s.platform    = 'java'
  s.authors     = ['Guy Boertje','Jerry Luk']
  s.email       = 'guyboertje@gmail.com'
  s.date        = '2011-09-16'
  s.summary     = 'A RabbitMQ client for JRuby'
  s.description = s.summary + ' with some AMQP gem api compatibility'
  s.homepage    = 'http://github.com/guyboertje/jessica'

  # = MANIFEST =
  s.files = %w[
    MIT-LICENSE
    README
    Rakefile
    jessica.gemspec
    lib/jessica.rb
    lib/jessica/LICENSE
    lib/jessica/LICENSE-APACHE2
    lib/jessica/LICENSE-GPL2
    lib/jessica/LICENSE-MPL-RabbitMQ
    lib/jessica/amqp_client_connector.rb
    lib/jessica/jars/commons-io-1.4.jar
    lib/jessica/jars/rabbitmq-client-tests.jar
    lib/jessica/jars/rabbitmq-client.jar
    lib/jessica/rabbitmq_client.rb
    lib/jessica/version.rb
    spec/rabbitmq_client_spec.rb
  ]
  # = MANIFEST =

  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }

  s.add_dependency 'require_all',               '~> 1.2.0'
  s.add_development_dependency 'awesome_print', '~> 0.4.0'
  s.add_development_dependency 'fuubar',        '~> 0.0.0'
  s.add_development_dependency 'rspec',         '~> 2.6.0'
end
