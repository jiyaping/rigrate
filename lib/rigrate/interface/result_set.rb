# encoding : utf-8

module Rigrate
  class ResultSet
    attr_accessor :db, :target_tbl_name
    attr_accessor :rows
    attr_accessor :column_info

    # join two table by given field { :jc => :job_code }
    def join(source_rs, key_fields = {})
      # TODO when key_fields is empty
      
      origin_rs_idx = column_idx(*key_fields.keys)
      source_rs_idx = source_rs.column_idx(*key_fields.values)

      ResultSet.new.tap do |rs|
        rs.column_info = column_info + source_rs.column_info

        # TODO remove addtion header
        # 

        rows.data.inject([]) do |new_rows, row|
          origin_rs_key_values = row.values(origin_rs_idx)

          selected_source_rs_row = source_rs.rows.select do |row|
            row.values(source_rs_idx) == origin_rs_key_values
          end

          # TODO remove addtion data columns

          if selected_source_rs_row.size == 0
            new_rows <<selected_source_rs_row.map { |t_row| row + t_row }
          else
            new_rows << row + Array.new(source_rs.column_info.size)
          end
        end
      end
    end

    # union two result set , columns defination will not change
    # default is union all style
    def union(target, opts = {})
      src_col_size = column_info.size
      target_col_size = target.column_info.size

      if src_col_size > target_col_size
        target.rows = fill_with_nil(target.rows, src_col_size - target_col_size)
      elsif src_col_size < target_col_size
        target.rows = target.rows.data.map { |row| row[0...src_col_size] }
      end

      rows + target.rows

      self
    end

    def migrate(src_rs, condition = nil, opts = {})
      # full table (direct insert)
      # part column update (need assoicate condition)

      condition = eval "{#{condition}}" unless condition.nil?
      unless condition.nil?
        src_cols_idx = column_idx(condition.keys)
        tg_cols_idx = column_idx(condition.values)

        rows = handler_row(src_rs.rows, src_cols_idx, tg_cols_idx)
      else
        rows = handler_row(src_rs.rows)
      end

      save!
    end

    # condition {:name => :c_name, :age => :age}
    # insert or update or delete
    def handler_row (src_rows, src_cols_idx = nil, tg_cols_idx = nil)
      new_rows_data = []

      if src_cols_idx.nil? && tg_cols_idx.nil?
        src_size = src_rows.first.size
        dest_size = column_info.size

        # delete all the dest rs data
        rows.map {|row| row.status = RowStatus::DELETE}

        src_rows = format_rows(src_rows)
        # make all rows NEW
        return src_rows.map {|row| row.status = RowStatus::NEW }
      end

      src_rows.each do |src_row|
        rows.each do |row|
          if column_values(src_cols_idx) == column_values(tg_cols_idx)
            # suppose column squence is the same
            if row.data == src_row.data
              new_rows_data << row
            else
              row.data = src_row.data
              row.status = RowStatus::UPDATED
              new_rows_data << row
            end
          else
            new_rows_data << Row.new(src_row.data, RowStatus::NEW)
          end
        end
      end

      new_rows_data
    end

    def save!
      begin
        # begin transation
        # handler delete
        handle_delete!
        # handler insert
        handle_insert!
        # handler update
        handle_update!
        # end transation
      rescue Exception => e
        raise e
        # rollback
      end
    end

    def handle_insert!
      sql = get_sql(:insert)

      op_rows = row.select do |row|
        row.status == RowStatus::NEW
      end

      op_rows.each do |row|
        db.insert sql, row.data
      end
    end

    def handle_update!
      sql = get_sql(:update)

      op_rows = rows.select do |row|
        row.status == RowStatus::UPDATED
      end

      op_rows.each do |row|
        db.update sql, row.data
      end
    end

    def handle_delete!
      sql = get_sql(:delete)

      op_rows = rows.select do |row|
        row.status == RowStatus::DELETE
      end

      op_rows.each do |row|
        db.delete sql, row.data
      end
    end

    def get_sql(type)
      case type
      when :insert
        params_str = column_info.map(&:name).join(',')
        values_str = Array.new(column_info.size){'?'}.join(',')

        "insert into #{target_tbl_name} (#{params_str}) values (#{values_str})"
      when :update
        setting_str, params_str = (column_info.map do |col|
          "#{col.name}=?"
        end).join(' and ')

        "update #{target_tbl_name} set #{setting_str} where #{params_str}"
      when :delete
        params_str = (column_info.map do |col|
          "#{col.name}=?"
        end).join(' and ')

        "delete from #{target_tbl_name} where #{params_str}"
      end
    end

    def format_rows(src_rows, tg_width, filled = nil)
      r_length = src_rows.first.size

      if r_length > tg_width
        src_rows.map do |row|
          row.data = [0..tg_width]
        end
      elsif r_length < tg_width
        src_rows.map do |row|
          row.data + Array.new(tg_width - r_length) { filled }
        end
      else
        src_rows
      end
    end

    private

    def column_values(row, cols)
      cols.map do |col|
        row[col]
      end
    end

    def column_idx(*names)
      names.inject([]) do |idxes, name|
        column_info.each_with_index do |col, idx|
          idxes << idx if col.name == name.to_s
        end

        idxes
      end
    end

    def fill_with_nil(rows, num)
      fill_row = Array.new(num)
      
      rows.map do |row|
        row + fill_row  
      end
    end
  end
end