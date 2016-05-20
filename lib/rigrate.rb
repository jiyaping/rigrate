# encoding : utf-8

require 'logger'

require 'rigrate/data_source'
require 'rigrate/error'
require 'rigrate/interface'
require 'rigrate/migration'
require 'rigrate/parser'

module Rigrate

  ##
  # used to output logger to mutli place
  #
  class MutiOutput
    def initialize(file, prompt)
      @target = []
      @target << file
      @target << STDOUT if prompt
    end

    def write(*args)
      @target.each { |dest| dest.write(*args) }
    end

    def close
      @target.each(&:close)
    end
  end

  # create rigrate main path
  path = File.join(Dir.home, ".rigrate")
  Dir.mkdir path unless (Dir.exist? path)

  @config = {
    :loglevel => 999, # default logger not show
    :logpath => path,
    :mode => :echo,
    :strict => false,
    :stdout => false,
    :ds => File.join(Dir.home, ".rigrate/ds"),
    :script => nil,
    :file => nil,
  }

  def self.run(opts = {})
    scripts = []
    configure(opts)

    # read all scripts <- from command line , file script , rigrate dir
    scripts << opts[:script] if opts[:script]
    scripts << File.read(opts[:file]) if opts[:file]
    if Dir.exist? opts[:dir].to_s     
      Dir["#{File.join(opts[:dir], '*')}"].each do |item|
        scripts << File.read(item)
      end
    end

    raise RigrateError.new("your should specify one script at least.") if scripts.size <= 0

    parser = Parser.new
    # loading data source
    if File.exist? config[:ds]
      parser.lex(File.read(config[:ds])).parsing
    end

    scripts.each do |script|
      parser.lex(script).parsing
    end
  end

  def self.logger
    logger_file = File.open(File.join(config[:logpath],logger_name), 'a')

    @log = Logger.new MutiOutput.new(logger_file, config[:stdout])
    @log.level = config[:loglevel]

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