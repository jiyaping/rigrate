# encoding : utf-8

gem 'minitest'
require 'minitest/autorun'
require 'rigrate'

class RowTest < Minitest::Test
  include Rigrate

  def setup
    @arr_1 = [1, 2, 3]
    @arr_2 = [1, 2, 4]

    @r1 = Row.new(@arr_1)
    @r2 = Row.new(@arr_2, RowStatus::NEW)
  end

  def test_new_row
    assert_equal @r1.data, @arr_1
  end

  def test_origin_row
    assert_equal @r1.status, RowStatus::ORIGIN
  end

  def test_row_equal
    refute_equal @r1, @r2
  end

  def test_row_plus
    r3 = Row.new(@arr_1 + @arr_2, RowStatus::UPDATED)
    assert (@r1 + @r2), r3
  end
end