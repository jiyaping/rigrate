# encoding : utf-8

require 'uri'

module Rigrate
  class DataSource
    attr_accessor :dbh

    def initialize(conn_uri)
      opts = extract_conn_param URI.parse(conn_uri)
      @dbh = eval(opts['db_type'].capitalize).new opts
    end

    def sql(str, *args)
      @dbh.select(str, *args)
    end

    def method_missing(mth, *args, &block)
      begin
        table_name = mth
        columns = args

        str_sql = build_sql(table_name, *columns)
        @dbh.select(str_sql, &block)
      rescue Exception => e
        raise e
      end
    end

    private

    def extract_conn_param(uri)
      opts = {}
      opts['db_type'] = uri.scheme if uri.scheme
      opts['hosts'] = uri.host if uri.host
      opts['username'] = uri.user if uri.user
      opts['password'] = uri.password if uri.password
      opts['port'] = uri.port if uri.port
      opts['db_name'] = uri.path.tr('/','') if uri.path.tr('/','').size > 0

      opts
    end

    def build_sql(table, *columns)
      columns = ['*'] if columns.size == 0

      "select #{columns.map(&:to_s).join(',')} from #{table}"
    end
  end
end