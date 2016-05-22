# encoding : utf-8

module Rigrate
  class ResultSet
    attr_accessor :db, :target_tbl_name
    attr_accessor :rows
    attr_accessor :column_info

    # join two table by given field { :jc => :job_code }
    def join(source_rs, key_fields = {})
      if key_fields.size <= 0
        raise ResultSetError.new("must specify the join condition.")
      end

      # convert condition key and value to string
      key_fields = key_fields.inject({}) do |h, (k, v)|
        h[k.to_s.upcase] = v.to_s.upcase
        h
      end

      origin_rs_idx = column_idx(*key_fields.keys)
      source_rs_idx = source_rs.column_idx(*key_fields.values)

      ResultSet.new.tap do |rs|
        # remove duplicate column header, base on column name
        addtion_column_info = source_rs.column_info.dup.delete_if do |col|
          key_fields.values.include? col.name.upcase
        end

        rs.column_info = @column_info + addtion_column_info

        rs.rows = @rows.inject([]) do |new_rows, row|
          origin_rs_key_values = row.values(*origin_rs_idx)

          selected_source_rs_row = source_rs.rows.select do |r|
            r.values(*source_rs_idx) == origin_rs_key_values
          end

          # remove duplicate data columns
          selected_source_rs_row.map! do |r|
            data = []
            r.data.each_with_index do |value, idx|
              data << value unless source_rs_idx.include? idx
            end

            Row.new data, RowStatus::NEW
          end
          # this is a left join.
          if selected_source_rs_row.size > 0
            selected_source_rs_row.each do |t_row|
              new_rows << Row.new(row.data + t_row.data, RowStatus::NEW)
            end
          else
            new_rows << row.dup.fill_with_nil(addtion_column_info.size)
          end

          new_rows
        end
      end
    end

    # union two result set , columns defination will not change
    # default is union all style
    def union(target, opts = {})
      src_col_size = column_info.size
      target_col_size = target.column_info.size

      # TODO need type checking?

      if src_col_size > target_col_size
        target.rows = target.rows.map do |row|
                        row.fill_with_nil(src_col_size - target_col_size)
                      end
      elsif src_col_size < target_col_size
        target.rows = target.rows.map { |row| row[0...src_col_size] }
      end

      @rows += target.rows

      self
    end

    def -(target, opts = {})
      src_col_size = column_info.size
      target_col_size = target.column_info.size

      # checking type? 
      #columns size must equal
      if src_col_size != target_col_size
        raise ResultSetError.new('minus must be used between column size equaled.')
      end
      @rows.reject! do |row|
        target.include? row
      end

      self
    end
    alias :minus :-

    # 
    # There are three modes
    # :echo is left to right ResultSet, and delete right which is not in left
    #    +When condition is nil+
    #       find the primary key as condition
    #       1. if primary key can't find in ResultSet then delete all record in right side, and insert
    #          all rows to left side
    #    +When condition is not nil+
    #      1. using condition to update existing row in right side 
    #      2. and insert rows which not included in right side
    #      3. then delete rows not in left side
    #
    # :contribute is left to right, and KEEP the file even it not in left
    #    +When condition is nil+
    #      find the primary key as condition
    #      1. if primary key can't find in Result then insert all rows to left side
    #    +When condition is not nil+
    #      1. using condition to update existing row in right side
    #      2. and insert rows not included in right side
    #
    # :sync will keep left and right the same +TODO+
    #    just keep two side the same
    #
    def migrate_from(src_rs, condition = nil, opts = {})
      Rigrate.logger.info("start migration : source rs [#{src_rs.size}] ->target rs [#{rows.size}]")
      mode = opts[:mode]
      condition = eval "{#{condition.to_s.upcase}}" unless condition.nil?

      if condition
        src_cols_idx = src_rs.column_idx(*condition.keys)
        tg_cols_idx = column_idx(*condition.values)
      else
        # delete line ->  src_cols_idx = src_rs.column_idx(*src_rs.default_migration_condition)
        # suppose all primary key of target resultset can be found in src result, and in same column idx
        tg_cols_idx = column_idx(*default_migration_condition)
        src_cols_idx = tg_cols_idx
      end
      @rows = handle_rows(src_rs.rows, mode, src_cols_idx, tg_cols_idx)
      save!(condition)
    end

    # condition {:name => :c_name, :age => :age}
    # insert or update or delete
    def handle_rows (src_rows_data, mode, src_cols_idx = nil, tg_cols_idx = nil)
      new_rows_data = []

      # condition parameter is null , so delete all ROWS. and then copy the source rs
      if src_cols_idx.to_a.size <= 0 && tg_cols_idx.to_a.size <= 0
        # TODO check the size
        if mode == :echo
          # delete all the dest rs data
          @rows.each do |row| 
            new_rows_data << Row.new(row.data, RowStatus::DELETE)
          end
        end
        # :echo and :contribute mode
        format_rows(src_rows_data, width).map do |row| 
          new_rows_data << Row.new(row.data, RowStatus::NEW)
        end
      else
        if mode == :echo
          new_rows_data += delete_not_exist_rows(@rows, src_rows_data, tg_cols_idx, src_cols_idx)
        end
        # :echo and :contribute
        new_rows_data += op_two_rows(@rows, tg_cols_idx, src_rows_data, src_cols_idx)
      end

      new_rows_data
    end

    def op_two_rows(target_rows, target_cols_idx, source_rows, src_cols_idx)
      result = []
      source_rows.each do |s_row|
        s_values = s_row.values(*src_cols_idx)
        fetched = false

        target_rows.each do |t_row|
          if s_values == t_row.values(*target_cols_idx)
            fetched = true
            if s_row.data != t_row.data
              result << Row.new(s_row.data, RowStatus::UPDATED)
            else
              result << Row.new(s_row.data, RowStatus::ORIGIN)
            end
          end
        end
        result << Row.new(s_row.data, RowStatus::NEW) unless fetched
      end

      result
    end

    def delete_not_exist_rows(target_rows, source_rows, target_cols_idx, src_cols_idx)
      result = []
      target_rows.each do |t_row|
        t_values = t_row.values(*target_cols_idx)
        fetched = false

        source_rows.each do |s_row|
          break fetched = true if s_row.values(*src_cols_idx) == t_values
        end
        result << Row.new(t_row.data, RowStatus::DELETE) unless fetched
      end

      result
    end

    def save!(condition = nil)
      begin
        # convert all to native row
        @rows.map do |row|
          convert_to_native_row(row)
        end

        @db.transaction if Rigrate.config[:strict]
        condition = condition.values if condition
        handle_delete!(condition)
        handle_insert!
        handle_update!(condition)
        @db.commit if @db.transaction_active?
      rescue Exception => e
        Rigrate.logger.error("saving resultset [#{rows.size}] error: #{e.message} #{e.backtrace}")
        raise e
        @db.rollback if @db.transaction_active?
      end
    end

    def handle_insert!()
      sql = get_sql(:insert)

      op_rows = @rows.select do |row|
        row.status == RowStatus::NEW
      end

      Rigrate.logger.info("start insert [#{op_rows.size}] rows using sql [#{sql}]")
      @db.insert sql, *op_rows if op_rows.size > 0
    end

    def handle_update!(condition = nil)
      key_fields = (condition || primary_key)
      sql = get_sql(:update, key_fields)
      param_fields = column_info.reject do |col|
        key_fields.include? col.name
      end.map { |col| col.name }

      op_rows = rows.select do |row|
        row.status == RowStatus::UPDATED
      end

      key_idx = column_idx(*key_fields)
      params_idx = column_idx(*param_fields)
      formated_rows = op_rows.map do |row|
                        key_values = row.values(*key_idx)
                        params_values = row.values(*params_idx)

                        params_values + key_values
                      end
      Rigrate.logger.info("start update [#{op_rows.size}] rows using sql [#{sql}]")
      @db.update sql, *formated_rows if formated_rows.size > 0
    end

    def handle_delete!(condition = nil)
      temp_primary_key = nil
      temp_primary_key = primary_key if primary_key.size > 0
      condi_fields = condition || temp_primary_key || column_info.map{ |col| col.name }

      sql = get_sql(:delete, condi_fields)

      op_rows = @rows.select do |row|
        row.status == RowStatus::DELETE
      end

      params_idx = column_idx(*condi_fields)
      formated_rows = op_rows.map do |row|
        row.values(*params_idx)
      end

      Rigrate.logger.info("start delete [#{op_rows.size}] rows using sql [#{sql}]")
      @db.delete sql, *formated_rows if formated_rows.size > 0
    end

    def get_sql(type, condition = nil)
      case type
      when :insert
        params_str = column_info.map(&:name).join(',')
        values_str = Array.new(column_info.size){'?'}.join(',')

        "insert into #{target_tbl_name} (#{params_str}) values (#{values_str})"
      when :update
        condi_fields = condition || primary_key
        params_str = condi_fields.map do |col|
          "#{col}=?"
        end.join(' and ')

        upd_fields = column_info.reject do |col|
          condi_fields.include? col.name
        end
        setting_str = upd_fields.map do |col|
          "#{col.name}=?"
        end.join(',')

        "update #{target_tbl_name} set #{setting_str} where #{params_str}"
      when :delete
        params_str = condition.map do |col|
          "#{col}=?"
        end.join(' and ')
        raise ResultSetError.new("can't get the delete sql") if params_str.to_s.size <= 0

        "delete from #{target_tbl_name} where #{params_str}"
      end
    end

    # convert source resulset rows to specify width
    def format_rows(src_rows, tg_width, filled = nil)
      r_length = src_rows.first.size

      if r_length > tg_width
        src_rows.map! do |row|
          row.data = row[0..tg_width]
        end
      elsif r_length < tg_width
        src_rows.map! do |row|
          row.fill_with_nil(tg_width - r_length)
        end
      end

      src_rows
    end

    def column_values(row, cols)
      cols.map do |col|
        row[col]
      end
    end

    def column_idx(*names)
      names.inject([]) do |idxes, name|
        column_info.each_with_index do |col, idx|
          idxes << idx if col.name.to_s.downcase == name.to_s.downcase
        end

        idxes
      end
    end

    def include?(p_row)
      @rows.each do |row|
        return true if row.data == p_row.data
      end

      false
    end

    def default_migration_condition
      flag = true
      cols = @column_info.map { |col| col.name }
      primary_key.each do |key|
        flag = false unless cols.include? key
      end

      primary_key if flag
    end

    def convert_to_native_row(row)
      @db.to_native_row(row, @column_info)
    end

    def size
      @rows.size
    end

    def primary_key
      @primary_key ||= @db.primary_key(@target_tbl_name)
    end

    private

    def fill_with_nil(rows, num)
      fill_row = Array.new(num)
      
      rows.map do |row|
        row + fill_row  
      end
    end

    def width
      @column_info.size
    end
  end
end