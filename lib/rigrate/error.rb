# encoding : utf-8

module Rigrate
  class BasicError < ::StandardError; end

  class InterfaceError < BasicError; end
  class ResultSetError < BasicError; end
end