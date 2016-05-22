# encoding : utf-8

require 'uri'

module Rigrate
  class DataSource
    attr_accessor :dbh

    def initialize(conn_uri)
      uri = URI.parse(conn_uri)
      klazz = uri.scheme.capitalize
      begin
        Module.const_get(klazz)
      rescue NameError
        DataSource.load_driver(klazz.downcase)                
      end
      @dbh = eval(klazz).new conn_uri
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

    def self.load_driver(driver_name)
      driver_path = File.expand_path(File.dirname(__FILE__) + "./interface/#{driver_name}")
      unless File.exist? "#{driver_path}.rb"
        raise InterfaceError.new("Driver [#{driver_name}] not found.")
      end

      require driver_path
    end

    def build_sql(table, *columns)
      columns = ['*'] if columns.size == 0

      "select #{columns.map(&:to_s).join(',')} from #{table}"
    end
  end
end