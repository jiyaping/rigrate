# encoding : utf-8

module Rigrate
  module Migration
    def union(rs_first_str, rs_second_str)
      if String === rs_first_str
        rs_first = instance_eval rs_first_str
      else
        rs_first = rs_first_str
      end
      rs_second = instance_eval rs_second_str

      if ResultSet === rs_first && ResultSet === rs_second
        Rigrate.logger.info("start union two ResultSet rs [#{rs_first.size}] <---> rs [#{rs_second.size}]")
        return rs_first.union rs_second
      else
        raise Exception.new('rs_first or rs_second is not a resultset')
      end
    end

    def minus(rs_first_str, rs_second_str)
      if String === rs_first_str
        rs_first = instance_eval rs_first_str
      else
        rs_first = rs_first_str
      end
      rs_second = instance_eval rs_second_str

      if ResultSet === rs_first && ResultSet === rs_second
        Rigrate.logger.info("start minus two ResultSet rs [#{rs_first.size}] <---> rs [#{rs_second.size}]")
        return rs_first.minus rs_second
      else
        raise Exception.new('rs_first or rs_second is not a resultset')
      end
    end

    def join(rs_first_str, rs_second_str, condition = nil)
      if String === rs_first_str
        rs_first = instance_eval rs_first_str
      else
        rs_first = rs_first_str
      end
      condition = eval("{#{condition}}") unless condition.nil?
      rs_second = instance_eval rs_second_str

      if ResultSet === rs_first && ResultSet === rs_second
        Rigrate.logger.info("start join two ResultSet rs [#{rs_first.size}] <---> rs [#{rs_second.size}]")
        return rs_first.join(rs_second, condition)
      else
        raise Exception.new('rs_first or rs_second is not a resultset')
      end
    end

    # migration mode
    def migrate(rs_first_str, rs_second_str, condition = nil, mode = nil)
      if String === rs_first_str
        rs_source = instance_eval rs_first_str 
      else
        rs_source = rs_first_str
      end
      rs_target = instance_eval rs_second_str

      default_opts = { :mode => Rigrate.config[:mode] }
      if mode
        default_opts[:mode] = instance_eval("#{mode}")
      end

      if ResultSet === rs_source && ResultSet === rs_target
        return rs_target.migrate_from(rs_source, condition, default_opts)
      else
        raise Exception.new('rs_target or rs_source is not a resultset.')
      end
    end

    def self_eval(rb_str)
      instance_eval(rb_str)
    end

    def data_source(name, conn_str)
      name = name.to_s
      ds = DataSource.new(conn_str)
      variable_name = "@#{name}".to_sym unless name.start_with? "@"
      instance_variable_set variable_name, ds
      instance_eval("def #{name}=(x); #{variable_name}=x; end;\
        def #{name}; #{variable_name}; end")
    end
    alias :ds :data_source
  end
end