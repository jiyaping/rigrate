require File.expand_path('../test_helper', __FILE__)

class OracleTest < TestHelper
  def setup
    @db = Oracle.new("oracle://scott:1234567@jtest")
    @db.db.autocommit = true

    # test for blob
    @db.exec("create table test_blob(id number(11), pic blob)")
    @db.exec("create table test_blob2(id number(11), pic blob)")

    # prepare one row
    @test_str = "xxxxxxxxxx"
    b_var = OCI8::BLOB.new(@db.db, @test_str)
    stm = @db.parse("insert into test_blob values (:1,:2)")
    stm.exec(1, b_var)
    stm.exec(2, b_var)

    # test for data migration
    @db.exec("create table user1(id number(11) primary key, name varchar(255), age number(4), flag varchar(255))")
    stm = @db.parse("insert into user1 values(:1, :2, :3, :4)")
    5.times do |i|
      stm.exec(i, "user1+#{i}", 20+i, "user1")
    end
    @db.exec("create table user2(id number(11) primary key, name varchar(255), age number(4), flag varchar(255))")
    stm = @db.parse("insert into user2 values(:1, :2, :3, :4)")
    5.times do |i|
      stm.exec(i+2, "user2+#{i}", 30+i, "user2")
    end
  end

  def test_new_connection
    assert @db
  end

  def test_respond_transaction
    assert_respond_to @db, :transaction_active?
  end

  def test_convert_question_mark_to_sym
    e_sql_1 = "select * from users where id = ?"
    a_sql_1 = "select * from users where id = :1"
    assert_equal a_sql_1, @db.convert_question_mark_to_symbol(e_sql_1, ['a'].size)

    e_sql_2 = "select * from users"
    a_sql_2 = "select * from users"
    assert_equal a_sql_2, @db.convert_question_mark_to_symbol(e_sql_2, [].size)

    e_sql_3 = "select * from users where id = ? and name = ?"
    a_sql_3 = "select * from users where id = :1 and name = :2"
    assert_equal a_sql_3, @db.convert_question_mark_to_symbol(e_sql_3, ['a', 'b'].size)
  end

  def test_blob_read_and_write
    stm = @db.parse("select * from test_blob")
    stm.exec
    row = stm.fetch
    assert_equal @test_str, row[1].read

    stm.exec
    row = stm.fetch
    rb_row = @db.to_rb_row(row)
    assert_equal [1, @test_str], rb_row
  end

  def test_select_rs
    rs = @db.select("select * from test_blob")
    assert_equal 2, rs.size
    assert_equal @test_str, rs.rows.first[1]
  end

  def test_oracle_migration
    str =<<SCRIPT
    ds :oa, "oracle://scott:1234567@jtest"
    ds :hr, "oracle://scott:1234567@jtest"
    
    from oa.user1 to hr.user2
SCRIPT

    parser = Parser.new
    parser.lex(str).parsing
    @rs = @db.select("select * from user2")
    assert_equal 5, @rs.size
    assert_equal 'user1', @rs.rows.first[3]
  end

  def test_blob_migration
    str =<<SCRIPT
    ds :oa, "oracle://scott:1234567@jtest"
    ds :hr, "oracle://scott:1234567@jtest"

    from oa.test_blob to hr.test_blob2
SCRIPT

    parser = Parser.new
    parser.lex(str).parsing
    @rs = @db.select("select * from test_blob2")
    assert_equal 2, @rs.size
    assert_equal @test_str, @rs.rows.first[1]
  end

  def teardown
    # clean environment
     @db.exec("drop table test_blob")
     @db.exec("drop table test_blob2")
     @db.exec('drop table user1')
     @db.exec('drop table user2')
  end
end