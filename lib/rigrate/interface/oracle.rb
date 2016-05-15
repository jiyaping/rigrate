# encoding : utf-8

require 'oci8'

module Rigrate
  class Oracle < Driver
    def initialize(uri)
      key_params = [:username, :password, :sid, :privilege]
      opts = params_pack(extract_from_db_path(uri), key_params)

      @db = OCI8.new(*opts)
    end

    def select(sql, *args)
      target_tbl_name = extract_tbl_from_sql(sql)
      sql = convert_question_mark_to_symbol(sql)

      ResultSet.new.tap do |rs|
        cursor = @db.parse(sql)
        rs.db = self
        rs.target_tbl_name = target_tbl_name
        rs.rows = []
        cursor.exec(*args) do |row|
          new_row = Row.new(to_rb_row(row))
          yield new_row if block_given?
          rs.rows << new_row
        end
        rs.column_info = statement_fields(cursor)
      end
    end

    def execute(sql, *args)
      sql = convert_question_mark_to_symbol(sql, args)

      cursor = @db.pares(sql)
      cursor.exec *args
    end
    alias :insert :execute
    alias :update :execute
    alias :delete :execute

    def statement_fields(cursor)
      cols = []

      cursor.column_metadata.each do |field|
        cols << Column.new(field.name, field.data_type)
      end
    end

    def convert_question_mark_to_symbol(sql, args)
      args.each_with_index do |arg, idx|
        sql.sub!('?', ":#{idx}")
      end

      sql
    end

    # TODO add blob/clob support 
    def to_native_row(row)
      row
    end

    def to_rb_row(row)
      row.map do |field|
        raw_type = [OCI8::BLOB, OCI8::CLOB, OCI8::NCLOB].select do |type|
          type === field
        end

        if raw_type.size > 0
          field.read
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

    def params_pack(hash, keys)
      keys.each do |key|
        hash.delete(key) unless hash.keys.include? key
      end

      hash
    end
  end
end