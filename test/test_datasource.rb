require File.expand_path('../test_helper', __FILE__)

class DataSourceTest < TestHelper
  def setup
    @ds = DataSource.new("sqlite://")

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
    @ds.dbh.execute_batch(setup_sql)
  end

  def test_new_data_srouce
    assert @ds
  end

  def test_build_sql
    assert_equal "select * from user", @ds.send(:build_sql, 'user')
    assert_equal "select name,age from user", @ds.send(:build_sql, 'user', 'name', 'age')
  end

  def test_extract_conn_param
    result = @ds.send(:extract_conn_param, URI.parse("mysql://localhost:3306"))
    assert_equal result.size, 3
    assert_equal result['db_type'], 'mysql'
  end

  def test_sql_block_given
    result = @ds.sql('select * from users') do |row|
      row
    end

    assert_equal 8, result.size
  end

  def test_sql
    result = @ds.sql('select * from users')

    assert_kind_of ResultSet, result
    assert_equal 8, result.size
  end

  def test_method_missing
    result = @ds.users(:name, :age) do |row|
      row
    end

    assert_kind_of ResultSet, result
    assert_equal 8, result.size
  end
end