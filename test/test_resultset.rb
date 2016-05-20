require File.expand_path('../test_helper', __FILE__)

class ResultSetTest < TestHelper
  def setup
    @tbl_name = 'users'

    @db =  Sqlite.new
    setup_sql =<<SQL
    create table users (
      id integer primary key not null,
      name varchar,
      age integer,
      birthday datetime default current_date
    );

    insert into users(id, name, age) values(1, 'jyp1', 23);
    insert into users(id, name, age) values(2, 'jyp2', 24);
    insert into users(id, name, age) values(3, 'jyp3', 25);
    insert into users(id, name, age) values(4, 'jyp4', 26);

    insert into users(id, name, age) values(5, 'jyp5', 23);
    insert into users(id, name, age) values(6, 'jyp6', 24);
    insert into users(id, name, age) values(7, 'jyp7', 25);
    insert into users(id, name, age) values(8, 'jyp8', 26);    
SQL

    # setup data
    @db.execute_batch(setup_sql)
    @rs = @db.select("select * from users where id < 5")
    @rs2 = @db.select("select * from users where id > 4")
  end

  def test_rs_attribute_db
    assert_instance_of Sqlite, @rs.db
  end

  def test_rs_attribute_target_tbl_name
    assert_equal 'users', @rs.target_tbl_name
  end

  def test_rs_attribute_rows
    assert_operator 0, :<, @rs.rows.size
  end

  def test_rs_attribute_rows_item_type
    assert_instance_of Array, @rs.rows.first.data
  end

  def test_fill_with_nil
    rows = [[1, 2], [2, 3]]
    assert_equal [[1, 2, nil], [2, 3, nil]], @rs.send(:fill_with_nil, rows, 1) 
  end

  def test_get_columns_idx
    assert_equal [1, 2], @rs.send(:column_idx, :name, :age)
  end

  def test_get_columns_idx2
    assert_equal [], @rs.send(:column_idx, :testtesttest)
  end

  def test_union_two_result
    assert_equal 8, @rs.union(@rs2).rows.size
  end

  def test_union_two_result_2
    rs1 = ResultSet.new.tap do |rs|
      rs.column_info = [Column.new(:name1, :type1),
                        Column.new(:name2, :type2)]
      rs.rows = [Row.new([111, 112]), 
                Row.new([121, 122])]
    end

    rs2 = ResultSet.new.tap do |rs|
      rs.column_info = [Column.new(:name5, :type3),
                        Column.new(:name6, :type4),
                        Column.new(:name7, :type5)]
      rs.rows = [Row.new([211, 212, 213]),
                Row.new([221, 222, 223])]
    end

    assert_equal 4, rs1.union(rs2).rows.size
  end

  def test_union_two_result_3
    rs1 = ResultSet.new.tap do |rs|
      rs.column_info = [Column.new(:name1, :type1),
                        Column.new(:name2, :type2)]
      rs.rows = [Row.new([111, 112]), 
                Row.new([121, 122])]
    end

    rs2 = ResultSet.new.tap do |rs|
      rs.column_info = [Column.new(:name5, :type3),
                        Column.new(:name6, :type4),
                        Column.new(:name7, :type5)]
      rs.rows = [Row.new([211, 212, 213]),
                Row.new([221, 222, 223])]
    end

    assert_equal 4, rs2.union(rs1).rows.size

    # all rows type are Rigrate::Row
    row = rs2.rows.select { |r| ! Row === r }
    assert_equal 0, row.size
  end

  def test_join_two_result_1
    assert_raises(ResultSetError) do
      @rs.join(@rs2)
    end
  end

  def test_join_two_resulset_2
    rs1 = ResultSet.new.tap do |rs|
      rs.column_info = [Column.new('name1', :type1),
                        Column.new('name2', :type2)]
      rs.rows = [Row.new([1, 112]), 
                Row.new([2, 122]),
                Row.new([3, 122])]
    end

    rs2 = ResultSet.new.tap do |rs|
      rs.column_info = [Column.new('name5', :type3),
                        Column.new('name6', :type4),
                        Column.new('name7', :type5)]
      rs.rows = [Row.new([1, 212, 213]),
                Row.new([2, 222, 223]),
                Row.new([2, 332, 333])]
    end

    rs = rs1.join(rs2, :name1 => :name5)
    # rows size is 3
    assert_equal 4, rs.rows.size
    # column size is 4
    assert_equal 4, rs.column_info.size
    # column name1 should fill with 2 nil
    row = rs.rows.select {|r| r[0] == 3}.first
    assert_equal 2, row.data.select {|field| field.nil?}.size
  end 

  def test_minus_1
    rs1 = ResultSet.new.tap do |rs|
      rs.column_info = [Column.new('name1', :type1),
                        Column.new('name2', :type2)]
      rs.rows = [Row.new([1, 112]), 
                Row.new([2, 122])]
    end

    rs2 = ResultSet.new.tap do |rs|
      rs.column_info = [Column.new('name5', :type3),
                        Column.new('name6', :type4)]
      rs.rows = [Row.new([1, 112])]
    end

    assert_equal 1, (rs1 - rs2).rows.size
  end

  def test_get_sql
    rs = @db.select('select id,name,age from users')


    # get insert 
    assert_equal "insert into users (id,name,age) values (?,?,?)", rs.get_sql(:insert)
    # get update
    assert_equal "update users set name=?,age=? where id=?", rs.get_sql(:update)
    assert_equal "update users set id=?,name=? where age=?", rs.get_sql(:update, ['age'])
    # get delete
    assert_equal "delete from users where id=?", rs.get_sql(:delete, ['id'])
  end

  def test_handle_delete
    rs_old = @db.select("select * from users")
    rs_old.rows.first.status = RowStatus::DELETE

    # delete the first row
    rs_old.handle_delete!
    rs_new = @db.select("select * from users")
    # result set size reduce 1
    assert_equal rs_old.size, (rs_new.size + 1)
  end

  def test_handle_update
    rs = @db.select("select * from users")
    # update the first row name of rs
    temp = 'xxxxxxxxxxx'
    rs.rows.first[1] = temp
    rs.handle_update!
    new_rs = @db.select('select * from users')
    assert_equal new_rs.rows.first[1], temp
  end

  def test_handle_update2
    rs = @db.select('select name,age from users')
    temp = 1000
    rs.rows.first[1] = temp
    rs.handle_update!(['name'])
    new_rs = @db.select('select name,age from users')
    assert_equal new_rs.rows.first[1], temp
  end

  def test_handle_insert
    rs = @db.select('select * from users')
    old_size = rs.size
    n_row = Row.new.tap do |row|
      row.data = [50, 'jiyaping', 25, '1990-08-02']
      row.status = RowStatus::NEW
    end
    rs.rows << n_row
    rs.handle_insert!

    new_rs = @db.select('select * from users')

    assert_equal new_rs.size, (old_size + 1)
    assert_equal new_rs.rows.last[1], 'jiyaping'
  end

  def test_handle_save
    rs = @db.select('select * from users')
    rs.rows.first.status = RowStatus::DELETE
    old_size = rs.size
    n_row = Row.new.tap do |row|
      row.data = [50, 'jiyaping', 25, '1990-08-02']
      row.status = RowStatus::NEW
    end
    rs.rows << n_row
    new_rs = @db.select('select * from users')
    assert_equal new_rs.size, old_size
  end

  def test_handle_row
    rs1 = @db.select('select * from users where id = 1')
    rs2 = @db.select('select * from users where id in (1,2)')
    mode = :echo
    rows = rs1.handle_rows(rs2.rows, mode)
    assert_equal 3, rows.size
    new_rows = rows.select do |r|
      r.status == RowStatus::NEW
    end

    upd_rows = rows.select do |r|
      r.status == RowStatus::UPDATED
    end

    del_rows = rows.select do |r|
      r.status == RowStatus::DELETE
    end

    assert_equal 2, new_rows.size
    assert_equal 0, upd_rows.size
    assert_equal 1, del_rows.size
  end

  def test_handle_row2
    rs1 = @db.select('select id,name,age from users where id = 1')
    rs2 = @db.select('select id idx,name,age from users where id =2')

    rows = rs1.handle_rows(rs2.rows, [0], [0])
    assert_equal 1, rows.size
    new_rows = rows.select do |r|
      r.status == RowStatus::NEW
    end

    upd_rows = rows.select do |r|
      r.status == RowStatus::UPDATED
    end

    del_rows = rows.select do |r|
      r.status == RowStatus::DELETE
    end

    orig_rows = rows.select do |r|
      r.status == RowStatus::DELETE
    end

    assert_equal 1, new_rows.size
    assert_equal 0, upd_rows.size
    assert_equal 0, del_rows.size
    assert_equal 0, orig_rows.size
  end

  def test_migrate_from
    rs1 = @db.select('select id,name,age from users')
    old_size = rs1.size
    rs2 = @db.select('select id idx,name,age from users')

    rs2.rows.first[1] = 'xxxxxxxx'
    rs2.rows << Row.new.tap do |row|
      row.status = RowStatus::NEW
      row.data = [10, 'jiyaping', 25]
    end

    rs2.rows.delete_at(3)

    rs1.migrate_from(rs2,nil,{:mode=>:echo})

    new_rs = @db.select('select id,name,age from users')
    assert_equal old_size, (new_rs.size)
  end
end