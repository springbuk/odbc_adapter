# frozen_string_literal: true

module ODBCAdapter
  module Adapters
    module Snowflake
      module SchemaStatements
        def current_database
          ActiveRecord::Base.logger.silence do
            begin
              exec_query('SELECT CURRENT_DATABASE() as current_database')[0]["current_database"].strip
            rescue ODBC_UTF8::Error
              []
            end
          end
        end

        def current_schema
          ActiveRecord::Base.logger.silence do
            begin
              exec_query('SELECT CURRENT_SCHEMA() as current_schema')[0]["current_schema"].strip
            rescue ODBC_UTF8::Error
              []
            end
          end
        end

        def table_exists?(table_name)
          tables.include?(format_case(table_name.to_s))
        end
      end
    end
  end
end
