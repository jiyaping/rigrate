# encoding : utf-8

require 'pg'

module Rigrate
  class Postgres
    def initialize(uri)
      default_opts = {
        host: '127.0.0.1',
        port: 5432,
        options: nil,
        tty: nil,
        dbname: nil,
        user: nil,
        password: nil,
      }

      default_opts.merge! extract_from_db_path(uri)
      @db = PG::Connecton.new(*default_opts)
    end

    def select(sql, *args)
      target_tbl_name = extract_tbl_from_sql(sql)
    end

    def primary_key(tbl_name)
      sql=<<STR
      SELECT a.attname
      FROM   pg_index i
      JOIN   pg_attribute a ON a.attrelid = i.indrelid
                           AND a.attnum = ANY(i.indkey)
      WHERE  i.indrelid = '#{tbl_name}'::regclass
      AND    i.indisprimary;
STR
      arr = []
      @db.exec(sql).each do |row|
        arr << row['attname']
      end
      arr
    end

    def convert_question_to_dollar_mark(sql, args)
      args.each_with_index do |arg, idx|
        sql.sub!('?', "$#{idx}")
      end

      sql
    end

    # postgre://jiyp:1234567@127.0.0.1/testdb?key1=val1&key2&val2
    def extract_from_db_path(uri)
      uri = URI.parse(uri)
      args = {}

      args[:host] = uri.host if uri.host
      args[:user] = uri.user if uri.user
      args[:password] = uri.password if uri.password
      args[:port] = uri.port if uri.port
      args[:scheme] = uri.scheme if uri.scheme
      args[:dbname] = uri.path.sub('/','') if uri.path.size > 1
      args[:options] = {}
      URI::decode_www_form(uri.query.to_s).to_h.each do |key, val|
        args[:options][key] = val
      end

      args
    end
  end
end