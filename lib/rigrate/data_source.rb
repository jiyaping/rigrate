# encoding : utf-8

require 'uri'

module Rigrate
  class DataSource
    attr_accessor :dbh

    def initialize(conn_uri)
      uri = URI.parse(conn_uri)
      @dbh = eval(uri.scheme.capitalize).new conn_uri
    end

    def sql(str, *args)
      begin
        @dbh.select(str, *args)
      rescue SQLite3::SQLException => e
        puts "DB: #{@dbh.inspect} SQL: #{str}"
        raise e
      end
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

    def build_sql(table, *columns)
      columns = ['*'] if columns.size == 0

      "select #{columns.map(&:to_s).join(',')} from #{table}"
    end
  end
end