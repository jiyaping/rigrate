# encoding : utf-8

module Rigrate
  # defination for column include name and type
  # column name is a string
  Column = Struct.new(:name, :type)

  class Driver
    attr_accessor :db

    def connect(db)
      self.db = db
    end

    def method_missing(mth, *args, &block)
      @db.send(mth, *args, &block)
    end

    def extract_conn_param(uri)
      opts = {}
      opts['db_type'] = uri.scheme if uri.scheme
      opts['host'] = uri.host if uri.host
      opts['username'] = URI.decode(uri.user) if uri.user
      opts['password'] = URI.decode(uri.password) if uri.password
      opts['port'] = uri.port if uri.port
      opts['db_name'] = uri.path.tr('/','') if uri.path.tr('/','').size > 0

      opts
    end

    def to_native_row(row, column_info)
      row
    end

    def extract_tbl_from_sql(sql_str)
      return $1 if sql_str =~ /from\s+(\w*)\s*/

      raise Exception.new('a lastest one table name must specify.')
    end
  end
end