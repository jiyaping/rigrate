gem "minitest"
require 'minitest/autorun'
require 'rigrate'

class TestHelper < MiniTest::Test
  include Rigrate
end