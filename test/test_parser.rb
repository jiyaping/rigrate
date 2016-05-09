require File.expand_path('../test_helper', __FILE__)

class ParserTest < TestHelper
  def setup
    @parser = Parser.new

    # define two real sqlite db in disk
    @hr = SQLite3::Database.new(File.join(Dir.tmpdir, 'hr.sqlite3'))
    @oa = SQLite3::Database.new(File.join(Dir.tmpdir, 'oa.sqlite3'))

    # fuck slowing .....
    @hr.execute("create table users(id integer, name varchar, age integer, flag varchar);")
    1.upto(5) do |i|
      @hr.execute("insert into users values(#{i}, 'hr#{i}', #{20 + i}, 'hr')")
    end

    @oa.execute("create table users(idx integer, name varchar, age integer, flag varchar);")
    3.upto(8) do |i|
      @oa.execute("insert into users values(#{i}, 'hr#{i}', #{20 + i}, 'oa')")
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

  def test_full_parser
    str =<<SCRIPT
    ds :oa, "sqlite://#{File.join(Dir.tmpdir, 'oa.sqlite3')}"
    ds :hr, "sqlite://#{File.join(Dir.tmpdir, 'hr.sqlite3')}"
SCRIPT
    
  end

  def teardown
    @hr.close
    @oa.close
    FileUtils.rm File.join(Dir.tmpdir, 'oa.sqlite3')
    FileUtils.rm File.join(Dir.tmpdir, 'hr.sqlite3') 
  end
end