# encoding : utf-8

require 'rigrate/data_source'
require 'rigrate/error'
require 'rigrate/interface'
require 'rigrate/migration'
require 'rigrate/parser'

module Rigrate
  def self.run(file, opts = {})
    script = File.read(file)
    parser = Parser.new
    parser.lex(script)
    parser.parsing
  end
end