# encoding : utf-8

require 'sqlite3'

module Rigrate
  class Sqlite < Driver
    def initialize(url = nil)
      url ||= "sqlite://memory"

      opts = extract_conn_param(URI.parse(url))
      file = extract_db_path(url)

      @db = ::SQLite3::Database.new(file, opts)
    end

    def select(sql, *args)
      target_tbl_name = extract_tbl_from_sql(sql)

      ResultSet.new.tap do |rs|
        stm = @db.prepare sql, *args

        rs.db = self
        rs.target_tbl_name = target_tbl_name
        rs.column_info = statement_fields(stm.columns, stm.types)
        rs.rows = []
        stm.execute.each do |row|
          new_row = Row.new(row.to_a)
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
        Rigrate.logger.error "SQL: #{sql} ARGS:#{args} -> #{e.backtrace}"
        raise e
      end
    end
    alias :insert :update
    alias :delete :update

    def primary_key(tbl_name)
      (@db.table_info(tbl_name).select do |col_hash|
        col_hash["pk"] == 1
      end).map do |col_hash|
        col_hash["name"]
      end
    end

    def statement_fields(names, types)
      cols = []
      names.each_with_index do |name, idx|
        cols << Column.new(name, types[idx])
      end

      cols
    end

    def extract_db_path(path)
      result = ":memory:"

      if path =~ /sqlite:\/\/(.*)/
        if $1 == 'memory'
          result = ":memory:"
        else
          result = $1
        end
      end

      result
    end

    def transaction
      @db.transaction
    end

    def commit
      @db.commit
    end
  end
end