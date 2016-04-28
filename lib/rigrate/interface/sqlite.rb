# encoding : utf-8

require 'sqlite3'

module Rigrate
  class SQLite < Driver
    def initialize(opts = {})
      default_opts = {
        file: ":memory:"
      }

      default_opts.merge! opts

      db = ::SQLite3::Database.new(*default_opts.values)
    end

    def select(sql, args = [])
      target_tbl_name = extract_tbl_from_sql(sql)

      ResultSet.new.tap do |rs|
        stm = db.prepare sql, *args

        rs.db = db
        rs.target_tbl_name = target_tbl_name
        rs.column_info = statement_fields(stm.columns, stm.types)
        stm.execute do |row|
          rs.rows << Row.new(row)
        end
      end
    end

    def insert(sql, *args)
      db.execute sql, *args
    end

    def primary_key(tbl_name)
      (db.table_info(tbl_name).select do |col_hash|
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
  end
end