# encoding : utf-8

require 'rigrate/interface/driver'
require 'rigrate/interface/result_set'
require 'rigrate/interface/row'
require 'rigrate/interface/sqlite'

module Rigrate
  def self.lazy_load_driver(driver_name)
    driver_path = "./interface/#{driver_name}"
    if File.exists? driver_path
      raise InterfaceError.new('Driver not found.')
    end

    require driver_path
  end
end

