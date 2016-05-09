# encoding : utf-8

require 'sqlite3'

module Rigrate
  class Sqlite < Driver
    def initialize(uri = nil)
      uri ||= URI.parse(default_uri)
      opts = params_format(uri)

      @db = ::SQLite3::Database.new(opts['file'], opts)
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

    def delete(sql, *args)
      @db.execute sql, *args
    end

    def update(sql, *args)
      @db.execute sql, *args
    end

    def insert(sql, *args)
      @db.execute sql, *args
    end

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

    def params_format(uri = {})
      args = {}

      if args['hosts'].downcase == 'memory'
        args['hosts'] = ':memory:'
      else
        File.join(args['hosts'], )
      end
      args['file'] = args['hosts'] if args['hosts']

      args
    end
  end
end