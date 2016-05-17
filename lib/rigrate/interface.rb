# encoding : utf-8

require 'uri'
require 'rigrate/interface/driver'
require 'rigrate/interface/result_set'
require 'rigrate/interface/row'
require 'rigrate/interface/sqlite'
require 'rigrate/interface/mysql'
require 'rigrate/interface/oracle'

module Rigrate
  def self.lazy_load_driver(driver_name)
    driver_path = "./interface/#{driver_name}"
    if File.exists? driver_path
      raise InterfaceError.new('Driver not found.')
    end

    require driver_path
  end
end