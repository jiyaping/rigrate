gem "minitest"
require 'minitest/autorun'
require 'fileutils'
require 'tmpdir'
require 'rigrate'

class TestHelper < MiniTest::Test
  include Rigrate

  DataSource.load_driver('mysql')
  DataSource.load_driver('oracle')
  DataSource.load_driver('sqlite')
end