require File.expand_path('../test_helper', __FILE__)

class SqliteTest < TestHelper
  def setup
    opts = {
      file: ":memory:"
    }

    @tbl_name = 'users'

    @db =  Sqlite.new(opts)
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
SQL

    # setup data
    @db.execute_batch(setup_sql)
  end

  def test_new_db
    assert @db
  end

  def test_table_users_exists?
    sql = "select name from sqlite_master\
     where type='table' and name='#{@tbl_name}'"

    result = @db.execute sql
    if result.size > 0
      q_tbl_name = result.first.first
    end

    assert_equal q_tbl_name, @tbl_name
  end

  def test_primary_key_returned_value
    result = @db.primary_key('users')

    assert Array === result
  end

  def test_get_users_primary_is_id
    result = @db.primary_key('users')

    assert_equal ['id'], result
  end

  def test_insert_values
    sql = "insert into users(id, name) values (1000, 'test')"
    assert @db.insert(sql)
    sql = "insert into users(id, name) values (?, ?)"
    args = [10001, 'testtest']
    assert @db.insert(sql, args)
  end

  def test_inherit_extract_tblname
    sql = "select * from users"

    assert_equal "users", @db.extract_tbl_from_sql(sql)
  end

  def test_select_is_success
    sql = "select id, name from users"
    assert @db.select sql
  end

  def test_delete
    assert @db.delete("delete from users where id=?", [1])
  end

  def teardown
    #@db.close unless @db.closed?
  end
end