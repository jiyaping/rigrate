require File.expand_path('../test_helper', __FILE__)

class DriverTest < TestHelper
  def setup
    @inst_str = "test test"
    @driver = Driver.new
    @driver.db = @inst_str
  end

  def test_driver_not_null
    assert @driver
  end

  def test_extract_tbl_from_sql
    tbl_name = "user"
    tbl_sql = "select * from user"

    assert_equal tbl_name, @driver.extract_tbl_from_sql(tbl_sql)
  end

  def test_extract_tbl_from_sql2
    tbl_name = "user"
    tbl_sql = "select * from   user where xxx = 'from'"

    assert_equal tbl_name, @driver.extract_tbl_from_sql(tbl_sql)
  end

  def test_extract_tbl_from_sql3
    tbl_name = "user"
    tbl_sql = "select * from   user from test" # invalid sql form

    assert_equal tbl_name, @driver.extract_tbl_from_sql(tbl_sql)
  end

  def test_method_missing
    execpt_size = @inst_str.length
    assert_equal execpt_size, @driver.length
  end
end