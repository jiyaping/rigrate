require File.expand_path('../test_helper', __FILE__)

class RowTest < TestHelper
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

  def test_add_item_to_row
    @r1.data << 1
    assert_equal @r1.size, @r1.data.size

    @r1 << 1
    assert_equal @r1.size, @r1.data.size
  end

  def test_row_equal
    refute_equal @r1, @r2
  end

  def test_row_plus
    r3 = Row.new(@arr_1 + @arr_2, RowStatus::UPDATED)
    assert_equal (@r1 + @r2).data.size, r3.size
  end

  def test_row_array_method_2
    assert_equal 1, @r1[0]
  end

  def test_get_values
    assert_equal [2, 3], @r1.values(1, 2)
  end

  def test_fill_with_nil
    # TODO
    assert_equal @r1 << nil, @r1.fill_with_nil(1)
  end
end