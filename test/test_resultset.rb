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

  def test_join_two_resulset
    assert @rs.join(@rs2)
  end
end


























