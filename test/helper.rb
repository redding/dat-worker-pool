# this file is automatically required when you run `assert`
# put any test helpers here

# add the root dir to the load path
$LOAD_PATH.unshift(File.expand_path("../..", __FILE__))

# require pry for debugging (`binding.pry`)
require 'pry'

require 'pathname'
ROOT_PATH = Pathname.new(File.expand_path('../..', __FILE__))

require 'logger'
TEST_LOGGER = Logger.new(ROOT_PATH.join("log/test.log"))

JOIN_SECONDS = 0.001

require 'test/support/factory'

# TODO: put test helpers here...
