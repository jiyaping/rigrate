require File.expand_path('../test_helper', __FILE__)

class Mysqltest < TestHelper
  def setup
    @db = Mysql.new("mysql://root:20080802@127.0.0.1/test")
    stm1 = @db.prepare(get_seed_data('tbl1'))
    stm1.execute
    stm1 = @db.prepare("insert into tbl1 values(?,?,?,?,?)")
    10.times do |i|
      args = [i, "jyp#{i}", 20+i, 'tbl1', "2001-01-#{1+i}"]
      stm1.execute(*args)
    end

    stm1 = @db.prepare(get_seed_data('tbl2'))
    stm1.execute
    stm1 = @db.prepare("insert into tbl2 values(?,?,?,?,?)")
    5.times do |i|
      args = [i, "jyp#{i}", 20+i, 'tbl2', "2001-02-#{1+i}"]
      stm1.execute(*args)
    end
  end

  def test_new_mysql
    assert @db
  end

  def test_transaction?
    assert_respond_to @db, :transaction_active?
  end

  def test_blob_read_and_write
    stm = @db.prepare("create table if not exists test_blob(id integer, pic blob)")
    stm.execute

    # insert blob
    test_str = "xxxxxxxxxxxxxxxxxxxxxxxxxxx"
    stm_i = @db.prepare("insert into test_blob values(?,?)")
    assert stm_i.execute(1, test_str)

    # read blob
    rs = @db.select("select * from test_blob")
    assert test_str, rs.rows.first[1]

    # clean db
    stm = @db.prepare("drop table test_blob")
    stm.execute
  end

  def test_mysql_migration
    str =<<SCRIPT
    ds :oa, "mysql://root:20080802@127.0.0.1/test"
    ds :oa_bak,"mysql://root:20080802@127.0.0.1/test"

    from oa.tbl1 to oa_bak.tbl2
SCRIPT
   
    parser = Parser.new
    parser.lex(str)
    parser.parsing
    rs = @db.select("select * from tbl2")
    assert_equal 10, rs.size
    assert_equal 'tbl1', rs.rows.first[3]
  end

  private

  def get_seed_data(tbl_name)
    prepare_a_data =<<SQL
    create table if not exists #{tbl_name}(
      id int,
      name varchar(255),
      age int,
      flag varchar(255),
      birth date
    );
SQL
    
    prepare_a_data
  end

  def teardown
    stm = @db.prepare("drop table tbl1")
    stm.execute
    stm = @db.prepare("drop table tbl2")
    stm.execute
  end
end