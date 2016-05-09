gem "minitest"
require 'minitest/autorun'
require 'fileutils'
require 'tmpdir'
require 'rigrate'

class TestHelper < MiniTest::Test
  include Rigrate
end