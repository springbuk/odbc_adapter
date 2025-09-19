# frozen_string_literal: true

module ODBCAdapter
  module Adapters
    module Snowflake
      class DatabaseMetadata < ODBCAdapter::DatabaseMetadata
        attr_reader :values
        def initialize
          p "SnowflakeMetadata Initialize"
          @values = {
            SQL_DBMS_NAME: 'Snowflake',
            SQL_DBMS_VER: '9.27.0',
            SQL_IDENTIFIER_CASE: ODBC::SQL_IC_UPPER,
            SQL_QUOTED_IDENTIFIER_CASE: 3,
            SQL_IDENTIFIER_QUOTE_CHAR: '"',
            SQL_MAX_IDENTIFIER_LEN: 255,
            SQL_MAX_TABLE_NAME_LEN: 255
          }.freeze
        end
      end
    end
  end

end
