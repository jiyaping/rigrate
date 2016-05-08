require File.expand_path('../test_helper', __FILE__)

class ParserTest < TestHelper
  def setup
    @parser = Parser.new
  end

  def test_lex
    tokens = @parser.lex("from oa.user to hr.account")

    assert_equal 4, tokens.size
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
end