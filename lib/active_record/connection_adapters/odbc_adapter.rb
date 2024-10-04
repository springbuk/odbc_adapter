require 'active_record'
require 'odbc'
require 'odbc_utf8'

require 'odbc_adapter/database_limits'
require 'odbc_adapter/database_statements'
require 'odbc_adapter/error'
require 'odbc_adapter/quoting'
require 'odbc_adapter/schema_statements'

require 'odbc_adapter/column'
require 'odbc_adapter/column_metadata'
require 'odbc_adapter/database_metadata'
require 'odbc_adapter/registry'
require 'odbc_adapter/version'

require 'odbc_adapter/type/type'
require 'odbc_adapter/concerns/concern'
require 'odbc_adapter/connect_common'

module ActiveRecord
  class Base
    class << self
      # Build a new ODBC connection with the given configuration.
      def odbc_connection(config)
        config = config.symbolize_keys

        connection, config =
          if config.key?(:dsn)
            ::ODBCAdapter::ConnectCommon.odbc_dsn_connection(config)
          elsif config.key?(:conn_str)
            ::ODBCAdapter::ConnectCommon.odbc_conn_str_connection(config)
          else
            raise ArgumentError, 'No data source name (:dsn) or connection string (:conn_str) specified.'
          end

        database_metadata = ::ODBCAdapter::DatabaseMetadata.new(connection, config[:encoding_bug])
        database_metadata.adapter_class.new(connection, logger, config, database_metadata)
      end
    end
  end

  module ConnectionAdapters
    class ODBCAdapter < AbstractAdapter
      include ::ODBCAdapter::DatabaseLimits
      include ::ODBCAdapter::DatabaseStatements
      include ::ODBCAdapter::Quoting
      include ::ODBCAdapter::SchemaStatements

      ADAPTER_NAME = 'ODBC'.freeze
      BOOLEAN_TYPE = 'BOOLEAN'.freeze
      VARIANT_TYPE = 'VARIANT'.freeze
      DATE_TYPE = 'DATE'.freeze
      JSON_TYPE = 'JSON'.freeze

      # ERR_DUPLICATE_KEY_VALUE                     = 23_505
      # ERR_QUERY_TIMED_OUT                         = 57_014
      # ERR_QUERY_TIMED_OUT_MESSAGE                 = /Query has timed out/
      # ERR_CONNECTION_FAILED_REGEX                 = '^08[0S]0[12347]'.freeze
      # ERR_CONNECTION_FAILED_MESSAGE               = /Client connection failed/
      # ERR_CONNECTION_UNAUTHENTICATED_MESSAGE = /Authentication token has expired\.  The user must authenticate again\./
      # ERR_SESSION_TIMOUT = /Session no longer exists\. New login required to access the service\./

      # The object that stores the information that is fetched from the DBMS
      # when a connection is first established.
      attr_reader :database_metadata

      def initialize(connection, logger, config, database_metadata)
        configure_time_options(connection)
        super(connection, logger, config)
        @database_metadata = database_metadata
        @raw_connection = connection
      end

      # Returns the human-readable name of the adapter.
      def adapter_name
        ADAPTER_NAME
      end

      # Does this adapter support migrations? Backend specific, as the abstract
      # adapter always returns +false+.
      def supports_migrations?
        true
      end

      # ODBC adapter does not support the returning clause
      def supports_insert_returning?
        false
      end

      # CONNECTION MANAGEMENT ====================================

      # Checks whether the connection to the database is still active. This
      # includes checking whether the database is actually capable of
      # responding, i.e. whether the connection isn't stale.
      def active?
        @raw_connection.connected?
      end

      # Disconnects from the database if already connected, and establishes a
      # new connection with the database.
      def reconnect
        disconnect!
        @raw_connection =
          if @config.key?(:dsn)
            ::ODBCAdapter::ConnectCommon.odbc_dsn_connection(@config)[0]
          else
            ::ODBCAdapter::ConnectCommon.odbc_conn_str_connection(@config)[0]
          end
        configure_time_options(@raw_connection)
      end
      alias reset! reconnect!

      # Disconnects from the database if already connected. Otherwise, this
      # method does nothing.
      def disconnect!
        @raw_connection.disconnect if @raw_connection.connected?
      end

      # Build a new column object from the given options. Effectively the same
      # as super except that it also passes in the native type.
      # rubocop:disable Metrics/ParameterLists
      def new_column(name, default, sql_type_metadata, null, native_type = nil, auto_incremented = false)
        ::ODBCAdapter::Column.new(name, default, sql_type_metadata, null, native_type, auto_incremented)
      end

      # Snowflake doesn't have a mechanism to return the primary key on inserts, it needs prefetched
      def prefetch_primary_key?(table_name = nil)
        true
      end

      def next_sequence_value(table_name = nil)
        exec_query("SELECT #{table_name}.NEXTVAL as new_id").first["new_id"]
      end

      def build_merge_sql(merge) # :nodoc:
        <<~SQL
          MERGE #{merge.into} AS TARGET USING (#{merge.values_list}) AS SOURCE ON #{merge.match}
          #{merge.merge_delete}
          #{merge.merge_update}
          #{merge.merge_insert}
        SQL
      end

      def exec_merge_all(sql, name) # :nodoc:
        exec_query(sql, name)
      end

      # odbc_adapter does not support returning, so there are no return values from an insert
      def return_value_after_insert?(column) # :nodoc:
        # If the column is an ODBC Adapter column then we can use the auto_incremented flag
        # otherwise, fallback to the default_function
        column.is_a?(::ODBCAdapter::Column) ? column.auto_incremented : column.auto_populated?
      end

      class << self
        private

        # Snowflake ODBC Adapter specific
        def initialize_type_map(map)
          map.register_type %r(boolean)i,               Type::Boolean.new
          map.register_type %r(date)i,                  Type::Date.new
          map.register_type %r(varchar)i,                Type::String.new
          map.register_type %r(time)i,                  Type::Time.new
          map.register_type %r(timestamp)i,              Type::DateTime.new
          map.register_type %r(binary)i,                Type::Binary.new
          map.register_type %r(double)i,                 Type::Float.new
          map.register_type(%r(decimal)i) do |sql_type|
            scale = extract_scale(sql_type)
            if scale == 0
              ::ODBCAdapter::Type::SnowflakeInteger.new
            else
              Type::Decimal.new(precision: extract_precision(sql_type), scale: scale)
            end
          end
          map.register_type %r(struct)i,                ::ODBCAdapter::Type::SnowflakeObject.new
          map.register_type %r(array)i,                 ::ODBCAdapter::Type::ArrayOfValues.new
          map.register_type %r(variant)i,               ::ODBCAdapter::Type::Variant.new
        end
      end

      TYPE_MAP = Type::TypeMap.new.tap { |m| initialize_type_map(m) }
      EXTENDED_TYPE_MAPS = Concurrent::Map.new

      protected

      # Translate an exception from the native DBMS to something usable by
      # ActiveRecord.
      # def translate_exception(exception, message:, sql:, binds:)
      #   error_number = exception.message[/^\d+/].to_i
      #   Rails.logger.debug 'ODBCAdapter: hit translate_exception with message #{exception.message}' 

      #   if error_number == ERR_DUPLICATE_KEY_VALUE
      #     ActiveRecord::RecordNotUnique.new(message, sql: sql, binds: binds)
      #   elsif error_number == ERR_QUERY_TIMED_OUT || exception.message =~ ERR_QUERY_TIMED_OUT_MESSAGE
      #     ::ODBCAdapter::QueryTimeoutError.new(message, sql: sql, binds: binds)
      #   elsif exception.message.match(ERR_CONNECTION_FAILED_REGEX) || exception.message =~ ERR_CONNECTION_FAILED_MESSAGE
      #     begin
      #       reconnect!
      #       ::ODBCAdapter::ConnectionFailedError.new(message, sql: sql, binds: binds)
      #     rescue => e
      #       puts "unable to reconnect #{e}"
      #     end
      #   elsif exception.message.match(ERR_CONNECTION_UNAUTHENTICATED_MESSAGE) || exception.message.match(ERR_SESSION_TIMOUT)
      #     Rails.logger.warn 'ODBCAdapter: Authentication token has expired. Attempting to reconnect.'
      #     reconnect!
      #     @raw_connection.run(sql)
      #   else
      #     super
      #   end
      # end

      private

      # Can't use the built-in ActiveRecord map#alias_type because it doesn't
      # work with non-string keys, and in our case the keys are (almost) all
      # numeric
      def alias_type(map, new_type, old_type)
        map.register_type(new_type) do |_|
          map.lookup(old_type)
        end
      end

      # Ensure ODBC is mapping time-based fields to native ruby objects
      def configure_time_options(connection)
        connection.use_time = true
      end
    end
  end
end
