# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "dat-worker-pool/version"

Gem::Specification.new do |gem|
  gem.name        = "dat-worker-pool"
  gem.version     = DatWorkerPool::VERSION
  gem.authors     = ["Collin Redding", "Kelly Redding"]
  gem.email       = ["collin.redding@me.com", "kelly@kellyredding.com"]
  gem.summary     = "A simple thread pool for processing generic 'work'"
  gem.description = "A simple thread pool for processing generic 'work'"
  gem.homepage    = "http://github.com/redding/dat-worker-pool"
  gem.license     = 'MIT'

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_development_dependency("assert", ["~> 2.16.3"])

  gem.add_dependency("much-timeout", ["~> 0.1.1"])

end
