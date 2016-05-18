# encoding : utf-8

require 'logger'

require 'rigrate/data_source'
require 'rigrate/error'
require 'rigrate/interface'
require 'rigrate/migration'
require 'rigrate/parser'

module Rigrate
  # create rigrate main path
  path = File.join(Dir.home, ".rigrate")
  Dir.mkdir path unless (Dir.exist? path)

  @config = {
    :logger_level => 1, # default logger not show
    :logger_path => path,
    :strict => false,
    :ds => File.join(Dir.home, ".rigrate/ds"),
    :script => nil,
  }

  def self.run(file, opts = {})
    configure(opts)
    script = File.read(file)
    parser = Parser.new
    # loading data source
    if File.exist? config[:ds]
      parser.lex(File.read(config[:ds])).parsing
    end

    parser.lex(script).parsing
  end

  def self.logger
    @log = Logger.new(File.join config[:logger_path],logger_name)
    @log.level = config[:logger_level]

    @log
  end

  def self.configure(opts)
    opts.each do |k, v|
      raise ParserError.new("arguments #{k} not valid.") unless config.keys.include? k.to_sym
    end

    config.merge! opts
  end

  def self.config
    @config
  end

  def self.logger_name
    "#{Time.now.strftime('%Y-%m-%d')}.log"
  end
end