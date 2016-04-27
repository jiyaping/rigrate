# encoding : utf-8

require 'uri'

module Rigrate
  class DataSource
    attr_accessor :dbh

    def initialize(conn_uri)
      opts = extract_conn_param URI.parse(conn_uri)
      dbh = eval(opts['db_type'].capitalize).new opts
    end

    def sql(str, *args)
      dbh.select(str, *args)
    end

    def method_missing(mth, *args, &block)
      begin
        table_name = mth
        columns = args

        str_sql = build_sql(table_name, columns)
        dbh.select(str_sql, *args)
      rescue Exception => e
        raise e
      end
    end

    private

    def extract_conn_param(uri)
      opts = {}
      opts['db_type'] = uri.scheme
      opts['hosts'] = uri.hosts
      opts['username'] = uri.username
      opts['password'] = uri.password
      opts['port'] = uri.port
      opts['db_name'] = uri.path.tr('/','')

      opts
    end

    def build_sql(table, columns)
      columns = '*' if columns.size == 0

      "select #{columns.map(&:to_s).join(',')} from #{table}"
    end
  end
end