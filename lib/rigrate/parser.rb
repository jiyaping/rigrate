# encoding : utf-8

module Rigrate
  class Parser
    include Migration
    
    module TokenType
      FROM      = :FROM_TAG
      TO        = :TO_TAG
      UNION     = :UNION_TAG
      JOIN      = :JOIN_TAG
      MINUS     = :MINUS_TAG
      ON        = :ON_TAG
      RUBY_STR  = :RUBY_STR_TAG
    end

    module StringType
      DOUBLE_QUOTE = :DQ_STR
      SINGLE_QUOTE = :SQ_STR
    end

    module LexStatus
      INIT        = :INIT_STATUS
      IN_KEYWORD  = :IN_KEYWORD_STATUS
      IN_RUBY_CODE= :IN_RUBY_CODE_STATUS
      IN_RUBY_STR = :IN_RUBY_STR_STATUS
    end

    Token = Struct.new(:type, :value)

    attr_accessor :tokens

    def lex(str)
      status = LexStatus::INIT
      @tokens = []
      t_token = ''
      t_sub_token = ''
      string_type = StringType::DOUBLE_QUOTE
      char_arr = str.chars
      loop do
        c = char_arr.shift

        if c == nil
          token = Token.new TokenType::RUBY_STR, t_token
          @tokens << token
          break
        end

        if status == LexStatus::IN_KEYWORD && c =~ /\s/
          if is_a_token?(t_token)
            @tokens << (Token.new get_token_type(t_token), t_token)
            t_token = ''
            t_sub_token = ''
            status = LexStatus::INIT
            next
          else
            status = LexStatus::IN_RUBY_CODE
          end
        end

        if status != LexStatus::IN_RUBY_CODE && status != LexStatus::IN_RUBY_STR
          (status = LexStatus::INIT && next) if c =~ /\s/
        end

        t_token << c
        t_sub_token << c

        if status == LexStatus::IN_RUBY_CODE ||
          status == LexStatus::IN_KEYWORD
          if c == '"'
            string_type = StringType::DOUBLE_QUOTE
            status = LexStatus::IN_RUBY_STR
            next
          elsif c == "'"
            string_type = StringType::SINGLE_QUOTE
            status = LexStatus::IN_RUBY_STR
            next
          end
        end

        if status == LexStatus::IN_RUBY_STR
          is_matched = false
          if (string_type == StringType::DOUBLE_QUOTE && c == '"') ||
             (string_type == StringType::SINGLE_QUOTE && c == "'")
            is_matched = true
          end
          
          if is_matched && t_token[-1] != "\\"
            status = LexStatus::IN_RUBY_CODE
          elsif c =~ /\s/
            t_sub_token = ''
          end
        end

        if status == LexStatus::IN_RUBY_CODE && c =~ /\s/
          if is_a_token? t_sub_token
            token = Token.new TokenType::RUBY_STR, t_token.sub(/#{t_sub_token}$/, '')
            @tokens << token
            token = Token.new get_token_type(t_sub_token), t_sub_token
            @tokens << token

            status = LexStatus::INIT
            t_token = ''
          end

          t_sub_token = ''
        end

        if status == LexStatus::INIT
          status = LexStatus::IN_KEYWORD
        end
      end

      # handler 

      @tokens
    end

    def parsing
      full_parse tokens.dup
    end

    #private

    def full_parse(tks)
      while tks.size > 0
        parse_rs_or_migrate_exp(tks)
      end
    end

    def parse_rs_or_migrate_exp(tks)
      token = tks.shift

      if token.type == TokenType::RUBY_STR
        v1 = self_eval token.value
      else
        tks.unshift token
        v1 = parse_migrate_exp(tks)
      end

      v1
    end

    def parse_migrate_exp(tks)
      token = tks.shift

      if token.type == TokenType::FROM
        v1 = parse_operate_exp(tks)

        sub_token = tks.shift
        if sub_token.type == TokenType::TO
          v2 = parse_rs_exp(tks)

          sub_token_1 = tks.shift
          if not sub_token_1.nil?
            if sub_token_1.type == TokenType::ON
              cond = tks.shift
              migrate(v1, v2, cond)
            else
              tks.unshift sub_token_1
              migrate(v1, v2)
            end
          else
            migrate(v1, v2)
          end
        end
      end
    end

    def parse_operate_exp(tks)
      v1 = parse_rs_exp(tks)

      while true
        token = tks.shift
        if token.type != TokenType::UNION &&
           token.type != TokenType::JOIN &&
           token.type != TokenType::MINUS
          tks.unshift token
          break
        end

        v2 = parse_rs_exp(tks)
        if token.type == TokenType::UNION
          v1 = union(v1, v2)
        elsif token.type == TokenType::MINUS
          v1 = minus(v1, v2) 
        elsif token.type == TokenType::JOIN
          sub_token = tks.shift

          if not sub_token.nil?
            if sub_token.type == TokenType::ON
              cond = tks.shift
              v1 = join(v1, v2, cond.value)
            else
              tks.unshift sub_token
              v1 = join(v1, v2)
            end
          else
            v1 = join(v1, v2)
          end
        end
      end

      v1
    end

    # TODO return value should change
    def parse_rs_exp(tks)
      token = tks.shift
      return if token.nil?

      if token.type == TokenType::RUBY_STR
        return token.value
      end
    end

    def is_a_token?(str)
      TokenType.constants.include? str.strip.upcase.to_sym
    end

    def get_token_type(str)
      TokenType.const_get(str.strip.upcase) 
    end
  end
end