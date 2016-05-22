Gem::Specification.new do |s|
  s.name        = 'rigrate'
  s.version     = '0.0.1'
  s.executables << 'rigrate'
  s.date        = '2016-04-01'
  s.summary     = 'Ruby Migrate! data migrate tool in ruby.'
  s.description = 'a data migrate tool between diffenect data sources write by ruby.'
  s.authors     = ['jiyaping']
  s.email       = 'jiyaping0802@gmail.com'
  s.homepage    = 'http://rubygems.org/gems/rigrate'
  s.license     = 'MIT'

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- test/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  s.add_dependency 'ruby-oci8', '~> 2.2'
  s.add_dependency 'sqlite3', '~> 1.3'
  s.add_dependency "mysql", "~> 2.9"
end