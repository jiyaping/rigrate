# encoding : utf-8

require 'mysql'

module Rigrate
  class Mysql < Driver
    attr_accessor :transaction_active

    def initialize(uri)
      default_opts = {
        host: '127.0.0.1',
        user: nil,
        passwd: nil,
        db: nil,
        port: 3306,
        socket: nil,
        flag: 0
      }

      extract_from_db_path(uri).each do |k, v|
        default_opts[k.to_sym] = v if default_opts.keys.include? k.to_sym
      end

      @db = ::Mysql.connect(*default_opts.values)
    end

    def select(sql, *args)
      target_tbl_name = extract_tbl_from_sql(sql)

      ResultSet.new.tap do |rs|
        stm = @db.prepare(sql)
        rs.db = self
        rs.target_tbl_name = target_tbl_name
        rs.column_info = statement_fields(stm)
        result = stm.execute(*args)
        rs.rows = []
        while row = result.fetch
          new_row = Row.new(to_rb_row(row))
          yield new_row if block_given?
          rs.rows << new_row
        end
      end
    end

    def save(resultset)
      resultset.db = self

      resultset.save!
    end

    def update(sql, *args)
      begin
        stm = @db.prepare sql
        args.each do |row|
          if Rigrate::Row === row
            row = row.data
          end
          stm.execute(*row)
        end
      rescue Exception => e
        Rigrate.logger.error "SQL: #{sql} ARGS:#{args}"
        raise e
      end
    end
    alias :insert :update
    alias :delete :update

    def primary_key(tbl_name)
      tbl_name = tbl_name.to_s

      (db.list_fields(tbl_name).fetch_fields.select do |field|
        field.is_pri_key?
      end).map(&:name)
    end

    def statement_fields(stm)
      cols = []

      stm.result_metadata.fetch_fields.each do |field|
        cols << Column.new(field.name, get_field_type(field.type))
      end
    end

    private

    def to_rb_row(mysql_row)
      mysql_row.map do |field|
        if ::Mysql::Time === field
          field.to_s
        else
          field
        end
      end
    end

    def format_sql_args(args)
      args.map do |arg|
        if String === arg
          "'#{arg}'"
        elsif DateTime === arg
          arg.strptime('%Y-%m-%d %H:%M:%S')
        else
          arg
        end
      end
    end

    def get_field_type(num)
      ::Mysql::Field.constants.select do |cons|
        cons if ::Mysql::Field.const_get(cons) == num
      end
    end

    def extract_from_db_path(uri)
      uri = URI.parse(uri)
      args = {}

      args[:host] = uri.host if uri.host
      args[:user] = uri.user if uri.user
      args[:passwd] = uri.password if uri.password
      args[:port] = uri.port if uri.port
      args[:scheme] = uri.scheme if uri.scheme
      args[:db] = uri.path.sub('/','') if uri.path.size > 1

      args
    end

    def transaction_active?
      @transaction_active
    end

    def transaction
      @db.autocommit false
      @transaction_active = true
    end

    def commit
      @db.commit
      @db.autocommit true
      @transaction_active = false
    end

    def rollback
      @db.rollback
      @db.autocommit true
      @transaction_active = false
    end
  end
end