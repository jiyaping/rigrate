require File.expand_path('../test_helper', __FILE__)

class MigrationTest < TestHelper
  def setup
    @obj = Object.new.extend(Migration)

    # seed data
    @obj.self_eval("ds :oa, 'sqlite://memory'")
    @obj.oa.dbh.execute_batch(get_seed_sql('oa'))

    @obj.self_eval("ds :hr, 'sqlite://memory'")
    @obj.hr.dbh.execute_batch(get_seed_sql('hr'))    
  end

  def test_self_eval
    assert_equal 3, @obj.self_eval('1+2')
  end

  def test_self_eval2
    assert_kind_of ResultSet, @obj.self_eval("oa.dbh.select('select * from users')")
  end

  def test_data_source
    @obj.ds('oa1', 'sqlite://memory')

    assert_kind_of DataSource, @obj.oa1
    assert @obj.oa1 = 'test'
    assert_equal @obj.oa1, 'test'
  end

  def test_union
    rs_first_str = "oa.sql('select id,name,age,flag from users')"
    rs_second_str = "hr.users(:id,:name,:age,:flag)"

    new_rs = @obj.union(rs_first_str, rs_second_str)
    assert_equal 10, new_rs.size
  end

  def test_join
    rs_first_str = "oa.sql('select id,name,age,flag from users where id in (1, 2)')"
    rs_second_str = "hr.sql('select id, birthday from users where id in (2, 3)')"
    condition = ":id => :id"
    new_rs = @obj.join(rs_first_str, rs_second_str, condition)

    assert_equal 2, new_rs.size

    # TODO when fetch none record , then fill with nil
  end

  def test_join_2
  end

  def test_migrate_full_table_migrate
    rs_first_str = "oa.sql('select * from users')"
    rs_second_str = "hr.users"

    # this will save the migrated resultset
    @obj.migrate(rs_first_str, rs_second_str)

    # search the db to confirm
    rs = @obj.oa.dbh.select("select flag from users")

    assert_equal 5, rs.size
    assert_equal 'oa', rs.rows.first[0]
  end

  def test_migrate_parted_record
    rs_first_str = "oa.sql('select id,name,flag from users where id in (2)')"
    rs_second_str = "hr.sql('select id,name,flag from users where id in (2)')"

    @obj.migrate(rs_first_str, rs_second_str)

    rs = @obj.oa.dbh.select("select count(*) nn from users where flag='hr'")

    assert_equal 1, rs.rows.first[0]
  end

  def test_migrate_parted_record
    # include delete, updated, insert (when target record exists in db but not in rs)
  end

  private

  def get_seed_sql(db, insert_times = 5)
    insert_sql = ""
    insert_times.times do |i|
      insert_sql << "insert into users(id, name, age, flag) values(#{i}, 'jyp#{i}', 26, '#{db}');"
    end

    setup_sql =<<SQL
    create table users (
      id integer primary key not null,
      name varchar,
      age integer,
      flag varchar,
      birthday datetime default current_date
    );
    #{insert_sql}
SQL

    setup_sql
  end
end






