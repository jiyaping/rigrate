# encoding : utf-8

require 'oci8'

module Rigrate
  class Oracle < Driver
    def initialize(uri)
      key_params = [:username, :password, :sid, :privilege]
      opts = params_pack(extract_from_db_path(uri), key_params)
      @db = OCI8.new(*opts.values)
    end

    def select(sql, *args)
      target_tbl_name = extract_tbl_from_sql(sql)
      sql = convert_question_mark_to_symbol(sql, args.size)

      ResultSet.new.tap do |rs|
        cursor = @db.parse(sql)
        rs.db = self
        rs.target_tbl_name = target_tbl_name
        rs.rows = []
        cursor.exec(*args)
        while row = cursor.fetch
          new_row = Row.new(to_rb_row(row))
          yield new_row if block_given?
          rs.rows << new_row
        end
        rs.column_info = statement_fields(cursor)
      end
    end

    def execute(sql, *args)
      sql = convert_question_mark_to_symbol(sql, args.first.size)

      cursor = @db.parse(sql)
      args.each do |row|
        if Rigrate::Row === row
          row = row.data
        end
        cursor.exec(*row)
      end
    end
    alias :insert :execute
    alias :update :execute
    alias :delete :execute

    def statement_fields(cursor)
      cols = []

      cursor.column_metadata.each do |field|
        cols << Column.new(field.name, field.data_type)
      end

      cols
    end

    def convert_question_mark_to_symbol(sql, param_size)
      param_size.times do |idx|
        sql.sub!('?', ":#{idx + 1}")
      end

      sql
    end

    # TODO add blob/clob support 
    def to_native_row(row, column_info)
      column_info.each_with_index do |col_info, idx|
        case col_info.type.to_s.downcase.to_sym
        when :blob
          new_val = OCI8::BLOB.new(@db, row[idx])
        when :clob
          new_val = OCI8::CLOB.new(@db, row[idx])
        when :bclob
          new_val = OCI8::NCLOB.new(@db, row[idx])
        when :date
          if row[idx]
            new_val = Time.new(row[idx])
          else
            new_val = ''
          end
        else
          new_val = row[idx]
        end

        if new_val.nil?
          new_val = ''
        end

        row.[]=(idx, new_val, false)
      end

      row
    end

    def to_rb_row(row)
      row.map do |field|
        type = field.class
        if [OCI8::BLOB, OCI8::CLOB, OCI8::NCLOB].include? type
          field.read
        elsif Time == type
          field.to_s          
        else
          field
        end
      end
    end

    # oracle://scott:tiger@foctest?privilege=:SYSOPER
    def extract_from_db_path(uri)
      uri = URI.parse(uri)
      args = {}

      args[:username] = uri.user if uri.user
      args[:password] = uri.password if uri.password
      args[:sid] = uri.host if uri.host
      URI::decode_www_form(uri.query.to_s).to_h.each do |key, val|
        args[key] = val
      end
      args
    end

    def primary_key(tbl_name)
      str =<<SQL
      select a.column_name 
       from user_cons_columns a, user_constraints b 
       where a.constraint_name = b.constraint_name 
       and b.constraint_type = 'P' 
      and a.table_name = '#{tbl_name}'
SQL

      result = []
      @db.exec(str) do |row|
        result << row.first
      end

      result
    end

    def params_pack(hash, keys)
      keys.each do |key|
        hash.delete(key) unless hash.keys.include? key
      end

      hash
    end

    def transaction_active?
      ! @db.autocommit?
    end

    def transaction
      @db.autocommit = false
    end

    def commit
      @db.commit
    end

    def rollback
      @db.rollbak
    end
  end
end