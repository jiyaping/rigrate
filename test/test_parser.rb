require File.expand_path('../test_helper', __FILE__)

class ParserTest < TestHelper
  def setup
    @parser = Parser.new

    # define two real sqlite db in disk
    @hr = SQLite3::Database.new(File.join(Dir.tmpdir, 'hr.sqlite3'))
    @oa = SQLite3::Database.new(File.join(Dir.tmpdir, 'oa.sqlite3'))

    # fuck slowing .....
    @hr.execute("create table users(id integer, name varchar, age integer, flag varchar);")
    5.times do |i|
      @hr.execute("insert into users values(#{i}, 'hrname#{i}', #{20 + i}, 'hr')")
    end

    @oa.execute("create table users(idx integer, name varchar, age integer, flag varchar);")
    10.times do |i|
      @oa.execute("insert into users values(#{i}, 'oaname#{i}', #{40 + i}, 'oa')")
    end
  end

  def test_lex
    tokens = @parser.lex("from oa.user to hr.account")

    assert_equal 4, tokens.size
    assert_equal 4, @parser.tokens.size
  end

  def test_lex2
    tokens = @parser.lex("FroM oa.sql('select * from user')\
     to hr.account on :jc=>:job_code")
    assert_equal 6, tokens.size
  end

  def test_lex3
    str =<<EOF
    from 
      oa.user(:id) 
      join
      hr.account(:job_code)
      union
      hr2.account(:test)
    to
      oa_test.user(:id)
    on :job_code=>:jc
EOF
   tokens = @parser.lex(str) 
   assert_equal 10, tokens.size
  end

  def test_parser_full_migration
    str =<<SCRIPT
    ds :oa, "sqlite://#{File.join(Dir.tmpdir, 'oa.sqlite3')}"
    ds :hr, "sqlite://#{File.join(Dir.tmpdir, 'hr.sqlite3')}"

    from oa.users to hr.users
SCRIPT
    
    # migration
    parser = Parser.new
    parser.lex(str)
    parser.parsing

    db = DataSource.new("sqlite://#{File.join(Dir.tmpdir, 'hr.sqlite3')}")
    rs = db.dbh.select("select * from users")
    assert_equal 10, rs.size
    assert_equal 'oa', rs.rows.first[3]
  end

  def test_parser_full_table_migration_with_columns
    str =<<SCRIPT
    ds :oa, "sqlite://#{File.join(Dir.tmpdir, 'oa.sqlite3')}"
    ds :hr, "sqlite://#{File.join(Dir.tmpdir, 'hr.sqlite3')}"

    from 
      oa.sql('select * from users') 
    to
      hr.users(:id, :name, :age, :flag)
SCRIPT
    
    # migration
    parser = Parser.new
    parser.lex(str)
    parser.parsing

    db = DataSource.new("sqlite://#{File.join(Dir.tmpdir, 'hr.sqlite3')}")
    rs = db.dbh.select("select * from users")
    assert_equal 10, rs.size
    assert_equal 'oa', rs.rows.first[3]
  end

  def test_parser_migration_with_single_condition
    str =<<SCRIPT
    ds :oa, "sqlite://#{File.join(Dir.tmpdir, 'oa.sqlite3')}"
    ds :hr, "sqlite://#{File.join(Dir.tmpdir, 'hr.sqlite3')}"

    from 
      oa.sql('select idx,name,flag from users where idx in (1,2)') 
    to
      hr.users(:id, :name, :flag)
    on :idx=>:id
SCRIPT
    
    # migration
    parser = Parser.new
    parser.lex(str)
    parser.parsing

    db = DataSource.new("sqlite://#{File.join(Dir.tmpdir, 'hr.sqlite3')}")
    rs = db.dbh.select("select * from users")
  end

  def test_parser_join_result
    str =<<SCRIPT
    ds :oa, "sqlite://#{File.join(Dir.tmpdir, 'oa.sqlite3')}"
    ds :hr, "sqlite://#{File.join(Dir.tmpdir, 'hr.sqlite3')}"
    ds :oa_alias, "sqlite://#{File.join(Dir.tmpdir, 'oa.sqlite3')}"

    from 
      oa.sql('select idx,name from users where idx=1 or idx = 2') 
      join
      oa_alias.sql('select idx,flag from users where idx=1 or idx=2')
      on :idx => :idx
    to
      hr.users(:id, :name, :flag)
SCRIPT

    # migration
    parser = Parser.new
    parser.lex(str)
    parser.parsing

    db = DataSource.new("sqlite://#{File.join(Dir.tmpdir, 'hr.sqlite3')}")
    rs = db.dbh.select("select * from users")
    assert_equal 2, rs.size
    assert_equal 'oa', rs.rows.first[3]
  end

  def test_parser_union_parted_fields
    str =<<SCRIPT
    ds :oa, "sqlite://#{File.join(Dir.tmpdir, 'oa.sqlite3')}"
    ds :hr, "sqlite://#{File.join(Dir.tmpdir, 'hr.sqlite3')}"
    ds :oa_alias, "sqlite://#{File.join(Dir.tmpdir, 'oa.sqlite3')}"

    from 
      oa.sql('select idx,name,flag from users where idx=1 or idx = 2') 
      union
      oa_alias.sql('select idx,name,flag from users where idx=2 or idx=3')
    to
      hr.users(:id, :name, :flag)
SCRIPT

    # migration
    parser = Parser.new
    parser.lex(str)
    parser.parsing

    db = DataSource.new("sqlite://#{File.join(Dir.tmpdir, 'hr.sqlite3')}")
    rs = db.dbh.select("select * from users")
    assert_equal 4, rs.size
    assert_equal 'oa', rs.rows.first[3]
  end

  def test_parser_minus_parted_fields
    str =<<SCRIPT
    ds :oa, "sqlite://#{File.join(Dir.tmpdir, 'oa.sqlite3')}"
    ds :hr, "sqlite://#{File.join(Dir.tmpdir, 'hr.sqlite3')}"
    ds :oa_alias, "sqlite://#{File.join(Dir.tmpdir, 'oa.sqlite3')}"

    from 
      oa.sql('select idx,name,flag from users where idx=1 or idx = 2') 
      minus
      oa_alias.sql('select idx,name,flag from users where idx=2 or idx=3')
    to
      hr.users(:id, :name, :flag)
SCRIPT

    # migration
    parser = Parser.new
    parser.lex(str)
    parser.parsing

    db = DataSource.new("sqlite://#{File.join(Dir.tmpdir, 'hr.sqlite3')}")
    rs = db.dbh.select("select * from users")
    assert_equal 1, rs.size
    assert_equal 'oa', rs.rows.first[3]
  end

  def test_multi_migration_task
    str =<<SCRIPT
    ds :oa, "sqlite://#{File.join(Dir.tmpdir, 'oa.sqlite3')}"
    ds :hr, "sqlite://#{File.join(Dir.tmpdir, 'hr.sqlite3')}"

    from oa.users to hr.users
    from hr.users to oa.users
SCRIPT

    # migration
    parser = Parser.new
    parser.lex(str)
    parser.parsing

    db = DataSource.new("sqlite://#{File.join(Dir.tmpdir, 'oa.sqlite3')}")
    db2 = DataSource.new("sqlite://#{File.join(Dir.tmpdir, 'hr.sqlite3')}")
    rs = db.dbh.select("select * from users")
    rs2 = db.dbh.select('select * from users')
    assert_equal 10, rs.size
    assert_equal 10, rs2.size
    assert_equal 'oa', rs.rows.first[3]
  end 

  def teardown
    @hr.close
    @oa.close
    FileUtils.rm File.join(Dir.tmpdir, 'oa.sqlite3')
    FileUtils.rm File.join(Dir.tmpdir, 'hr.sqlite3') 
  end
end