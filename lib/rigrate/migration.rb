# encoding : utf-8

module Rigrate
  module Migration
    def union(rs_first_str, rs_second_str)
      rs_first = instance_eval rs_first_str
      rs_second = instance_eval rs_second_str

      if ResultSet === rs_first && ResultSet === rs_second
        return rs_first.union rs_second
      else
        raise Exception.new('rs_first or rs_second is not a resultset')
      end
    end

    def join(rs_first_str, rs_second_str, condition = nil)
      condition = eval("{#{condition}}") unless condition.nil?

      rs_first = instance_eval rs_first_str
      rs_second = instance_eval rs_second_str

      if ResultSet === rs_first && ResultSet === rs_second
        return rs_first.join(rs_second, condition)
      else
        raise Exception.new('rs_first or rs_second is not a resultset')
      end
    end

    # migration mode
    # 1. 增量插入
    # 2. 全表插入 condition
    # 3. 更新
    def migrate(rs_first_str, rs_second_str, condition = nil)
      rs_source = instance_eval rs_first_str
      rs_target = instance_eval rs_second_str

      condition = eval "{#{condition}}" unless condition.nil?
      if ResultSet === rs_source && ResultSet === rs_target
        return rs_source.migrate(rs_target, condition)
      else
        raise Exception.new('rs_target or rs_source is not a resultset.')
      end
    end

    def self_eval(rb_str)
      module_eval(rb_str)
    end

    def data_source(name, conn_str)
      ds = DataSource.new(conn_str)

      module_eval("attr_accessor #{name.to_sym}; name = ds")
    end
    alias :ds :data_source
  end
end