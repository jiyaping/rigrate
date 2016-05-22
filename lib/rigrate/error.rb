# encoding : utf-8

module Rigrate
  class BasicError < ::StandardError; end

  class RigrateError < BasicError; end
  class ParserError < BasicError; end
  class InterfaceError < BasicError; end
  class ResultSetError < BasicError; end
  class DirverError < BasicError; end
end