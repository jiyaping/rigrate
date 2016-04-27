# encoding : utf-8

module Rigrate
  def self.lazy_load_driver(driver_name)
    driver_path = "./interface/#{driver_name}"
    if File.exists? driver_path
      raise InterfaceError.new('Driver not found.')
    end

    require driver_path
  end
end

