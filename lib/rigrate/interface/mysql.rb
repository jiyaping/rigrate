# encoding : utf-8

require 'mysql'

module Rigrate
  class MysqlDriver < Driver
    def initialize(opts = {})
      default_opts = {
        host: nil,
        user: nil,
        passwd: nil,
        db: nil,
        port: 3306,
        socket: nil,
        flag: 0
      }

      default_opts.merge! opts

      db = ::Mysql.connect(*default_opts.values)
    end

    def select(sql, *args)
      if args.size > 0
        args = format_sql_args(args)

        args.each do |arg|
          sql.sub!(/(\?)/, arg)
        end
      end

      target_tbl_name = extract_tbl_from_sql(sql)

      ResultSet.new.tap do |rs|
        stm = db.prepare(sql)
        rs.db = db
        rs.target_tbl_name = target_tbl_name
        rs.column = statement_fields(stm)
        stm.execute *args
        stm.fetch do |row|
          rs << Row.new(to_rb_row(row))
        end
      end
    end

    def insert(sql, *args)
      stm = db.prepare(sql)
      stm.execte args
    end

    def primary_key(tbl_name)
      tbl_name = tbl_name.to_s

      (db.list_fields(tbl_name).fetch_fields.select do |field|
        field.is_pri_key?
      end).map(&:name)
    end

    def statement_fields(stm)
      cols = []

      stm.result_datameta.fetch_fields.each do |field|
        cols << Column.new(field.name, get_field_type field.type )
      end
    end

    def convert_from_rb_type(row)
      
    end

    def convert_to_rb_type(row)

    end

    private

    def to_rb_row(mysql_row)
      mysql_row.map do |cell|
        if Mysql::Time === cell
          DateTime.strptime(cell.to_s, '%Y-%m-%d %H:%M:%S')
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
  end
end