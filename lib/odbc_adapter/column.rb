module ODBCAdapter
  class Column < ActiveRecord::ConnectionAdapters::Column
    attr_reader :native_type, :auto_incremented

    # Add the native_type accessor to allow the native DBMS to report back what
    # it uses to represent the column internally.
    # rubocop:disable Metrics/ParameterLists
    def initialize(name, default, sql_type_metadata = nil, null = true, native_type = nil, auto_incremented, **kwargs)
      super(name, default, sql_type_metadata, null, **kwargs)
      @native_type = native_type
      @auto_incremented = auto_incremented
    end
  end
end
