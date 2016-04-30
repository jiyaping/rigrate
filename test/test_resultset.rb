require File.expand_path('../test_helper', __FILE__)

class ResultSetTest < TestHelper
  def setup
    opts = {
      file: ":memory:"
    }

    @tbl_name = 'users'

    @db =  SQLite.new(opts)
    setup_sql =<<SQL
    create table users (
      id integer primary key not null,
      name varchar,
      age integer,
      birthday datetime default current_date
    );

    insert into users(id, name, age) values(1, 'jyp1', 23);
    insert into users(id, name, age) values(2, 'jyp2', 24);
    insert into users(id, name, age) values(3, 'jyp3', 25);
    insert into users(id, name, age) values(4, 'jyp4', 26);

    insert into users(id, name, age) values(5, 'jyp5', 23);
    insert into users(id, name, age) values(6, 'jyp6', 24);
    insert into users(id, name, age) values(7, 'jyp7', 25);
    insert into users(id, name, age) values(8, 'jyp8', 26);    
SQL

    # setup data
    @db.execute_batch(setup_sql)
    @rs = @db.select("select * from users where id < 5")
    @rs2 = @db.select("select * from users where id > 4")
  end

  def test_rs_attribute_db
    assert_instance_of SQLite3::Database, @rs.db
  end

  def test_rs_attribute_target_tbl_name
    assert_equal 'users', @rs.target_tbl_name
  end

  def test_rs_attribute_rows
    assert_operator 0, :<, @rs.rows.size
  end

  def test_rs_attribute_rows_item_type
    assert_instance_of Array, @rs.rows.first.data
  end

  def test_fill_with_nil
    rows = [[1, 2], [2, 3]]
    assert_equal [[1, 2, nil], [2, 3, nil]], @rs.send(:fill_with_nil, rows, 1) 
  end

  def test_get_columns_idx
    origin_columns = ['id', 'name', 'age', 'birthday']

    assert_equal [1, 2], @rs.send(:column_idx, :name, :age)
  end

  def test_get_columns_idx2
    origin_columns = ['id', 'name', 'age', 'birthday']
    assert_equal [], @rs.send(:column_idx, :testtesttest)
  end

  def test_union_two_result
    assert_equal 8, @rs.union(@rs2).rows.size
  end

  def test_union_two_result_2
    rs1 = ResultSet.new.tap do |rs|
      rs.column_info = [Column.new(:name1, :type1),
                        Column.new(:name2, :type2)]
      rs.rows = [Row.new([111, 112]), 
                Row.new([121, 122])]
    end

    rs2 = ResultSet.new.tap do |rs|
      rs.column_info = [Column.new(:name5, :type3),
                        Column.new(:name6, :type4),
                        Column.new(:name7, :type5)]
      rs.rows = [Row.new([211, 212, 213]),
                Row.new([221, 222, 223])]
    end

    assert_equal 4, rs1.union(rs2).rows.size
  end

  def test_union_two_result_3
    rs1 = ResultSet.new.tap do |rs|
      rs.column_info = [Column.new(:name1, :type1),
                        Column.new(:name2, :type2)]
      rs.rows = [Row.new([111, 112]), 
                Row.new([121, 122])]
    end

    rs2 = ResultSet.new.tap do |rs|
      rs.column_info = [Column.new(:name5, :type3),
                        Column.new(:name6, :type4),
                        Column.new(:name7, :type5)]
      rs.rows = [Row.new([211, 212, 213]),
                Row.new([221, 222, 223])]
    end

    assert_equal 4, rs2.union(rs1).rows.size

    # all rows type are Rigrate::Row
    row = rs2.rows.select { |r| ! Row === r }
    assert_equal 0, row.size
  end

  def test_join_two_result_1
    assert_raises(ResultSetError) do
      @rs.join(@rs2)
    end
  end

  def test_join_two_resulset_2
    rs1 = ResultSet.new.tap do |rs|
      rs.column_info = [Column.new('name1', :type1),
                        Column.new('name2', :type2)]
      rs.rows = [Row.new([1, 112]), 
                Row.new([2, 122])]
    end

    rs2 = ResultSet.new.tap do |rs|
      rs.column_info = [Column.new('name5', :type3),
                        Column.new('name6', :type4),
                        Column.new('name7', :type5)]
      rs.rows = [Row.new([1, 212, 213]),
                Row.new([2, 222, 223]),
                Row.new([2, 332, 333])]
    end

    rs = rs1.join(rs2, :name1 => :name5)
    # rows size is 3
    assert_equal 3, rs.rows.size
    # column size is 4
    assert_equal 4, rs.column_info.size
  end

  def test_minus_1
    rs1 = ResultSet.new.tap do |rs|
      rs.column_info = [Column.new('name1', :type1),
                        Column.new('name2', :type2)]
      rs.rows = [Row.new([1, 112]), 
                Row.new([2, 122])]
    end

    rs2 = ResultSet.new.tap do |rs|
      rs.column_info = [Column.new('name5', :type3),
                        Column.new('name6', :type4)]
      rs.rows = [Row.new([1, 112])]
    end

    assert_equal 1, (rs1 - rs2).rows.size
  end
end









