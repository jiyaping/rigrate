# encoding : utf-8

module Rigrate
	module RowStatus
    NEW     =  :NEW
    UPDATED =  :UPDATED
    DELETE  =  :DELETE
    ORIGIN  =  :ORIGIN
  end

  class Row < DelegateClass(Array)
    attr_accessor :data
    attr_accessor :status
    attr_accessor :fields

    def initialize(data = [], status = RowStatus::ORIGIN)
      self.data = data
      self.status = status

      super(data)
    end

    def values(*idxes)
      idxes.map do |idx|
        self[idx]
      end
    end

    # why need in this way?
    def +(t_row)
      t_row.data.each { |item| @data << item }
      @status = RowStatus::UPDATED

      self
    end

    def ==(t_row)
      @data == t_row.data && status == t_row.status
    end

    def fill_with_nil(num)
      @data + Array.new(num)

      self
    end
  end
end