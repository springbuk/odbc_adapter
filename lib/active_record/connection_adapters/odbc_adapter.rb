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
require 'aws_secrets_manager'

module ActiveRecord
  class Base
    class << self
      # Build a new ODBC connection with the given configuration.
      def odbc_connection(config)
        config = config.symbolize_keys

        connection, config =
          if config.key?(:dsn)
            odbc_dsn_connection(config)
          elsif config.key?(:conn_str)
            odbc_conn_str_connection(config)
          else
            raise ArgumentError, 'No data source name (:dsn) or connection string (:conn_str) specified.'
          end

        database_metadata = ::ODBCAdapter::DatabaseMetadata.new(connection, config[:encoding_bug])
        database_metadata.adapter_class.new(connection, logger, config, database_metadata)
      end

      private

      # Connect using a predefined DSN.
      def odbc_dsn_connection(config)
        username   = config[:username] ? config[:username].to_s : nil
        password   = config[:password] ? config[:password].to_s : nil
        odbc_module = config[:encoding] == 'utf8' ? ODBC_UTF8 : ODBC
        connection = odbc_module.connect(config[:dsn], username, password)

        # encoding_bug indicates that the driver is using non ASCII and has the issue referenced here https://github.com/larskanis/ruby-odbc/issues/2
        [connection, config.merge(username: username, password: password, encoding_bug: config[:encoding] == 'utf8')]
      end

      # Connect using ODBC connection string
      # Supports DSN-based or DSN-less connections
      # e.g. "DSN=virt5;UID=rails;PWD=rails"
      #      "DRIVER={OpenLink Virtuoso};HOST=carlmbp;UID=rails;PWD=rails"
      def odbc_conn_str_connection(config)
        attrs = config[:conn_str].split(';').map { |option| option.split('=', 2) }.to_h
        odbc_module = attrs['ENCODING'] == 'utf8' ? ODBC_UTF8 : ODBC

        # The connection string may specify an AWS secret key id as the value of PRIV_KEY_FILE. Development environmnets typically just use a filepath of a static key file.
        aws_secret_id = attrs['PRIV_KEY_FILE'].start_with?(Rails.root.to_s) ? nil : attrs['PRIV_KEY_FILE']

        # when called from reconnect a driver may already be defined
        driver = config[:driver] || odbc_module::Driver.new

        puts "==========> odbc_connection: Building connection using connection string: #{config[:conn_str]}"

        # Skip setting up the driver if it is already set (reconnect case)
        if (!config[:driver])
          puts "==========> NO existing driver in config, Initializing driver..."
          driver.name = 'odbc'
          driver.attrs = attrs
          if aws_secret_id
            AwsSecretsManager.configure_driver(driver, aws_secret_id)
          end
        end

        begin
          puts "==========> Connecting with key file: #{driver.attrs['PRIV_KEY_FILE']}"
          # TODO:possibly add the ability to obtain a lock from the AWS Secrets Manager here so we can wrap the connect attempt in a lock
          connection = odbc_module::Database.new.drvconnect(driver)
        rescue odbc_module::Error => e
          # If the connection string specifies an AWS secret key id as the value of PRIV_KEY_FILE (instead of a filepath as used in development environments)
          # then attempt to fetch the latest private key file from AWS, serialize it and attempt to connect again. Local files are identified by a value starting with Rails.root
          # (such as '/path/to/private_key.pem')
          puts "==========> Handling connection error:\n#{e.class}-#{e.message}"
          if aws_secret_id && e.message.include?("private key")
            AwsSecretsManager.refresh_key_file(aws_secret_id)
            puts "==========> Attempting reconnect after refresh of key file"
            connection = odbc_module::Database.new.drvconnect(driver)
          # TEST SCENARIO BELOW WHERE A BAD LOCAL KEY WAS SPECIFIED IN THE CONNECTION STRING
          # elsif e.message.include?("Error loading private key file") && Rails.env.development?
          #   driver.attrs['PRIV_KEY_FILE'] = Rails.root.join(AwsSecretsManager::KEY_FILE_LOCAL_NAME).to_s
          #   puts "==========> Attempting reconnect with new private key file: #{driver.attrs['PRIV_KEY_FILE']}"
          #   connection = odbc_module::Database.new.drvconnect(driver)
          ########## END TEST SCENARIO CODE ############
          else
            raise e
          end
        end

        # encoding_bug indicates that the driver is using non ASCII and has the issue referenced here https://github.com/larskanis/ruby-odbc/issues/2
        [connection, config.merge(driver: driver, encoding: attrs['ENCODING'], encoding_bug: attrs['ENCODING'] == 'utf8')]
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

      ERR_DUPLICATE_KEY_VALUE                     = 23_505
      ERR_QUERY_TIMED_OUT                         = 57_014
      ERR_QUERY_TIMED_OUT_MESSAGE                 = /Query has timed out/
      ERR_CONNECTION_FAILED_REGEX                 = '^08[0S]0[12347]'.freeze
      ERR_CONNECTION_FAILED_MESSAGE               = /Client connection failed/

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
        # odbc_module = @config[:encoding] == 'utf8' ? ODBC_UTF8 : ODBC
        @raw_connection =
          if @config.key?(:dsn)
            # odbc_module.connect(@config[:dsn], @config[:username], @config[:password])
            odbc_dsn_connection(@config)[0]
          else
            # odbc_module::Database.new.drvconnect(@config[:driver])
            odbc_conn_str_connection(@config)[0]
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

      #Snowflake doesn't have a mechanism to return the primary key on inserts, it needs prefetched
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
        #Snowflake ODBC Adapter specific
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
      def translate_exception(exception, message:, sql:, binds:)
        error_number = exception.message[/^\d+/].to_i

        if error_number == ERR_DUPLICATE_KEY_VALUE
          ActiveRecord::RecordNotUnique.new(message, sql: sql, binds: binds)
        elsif error_number == ERR_QUERY_TIMED_OUT || exception.message =~ ERR_QUERY_TIMED_OUT_MESSAGE
          ::ODBCAdapter::QueryTimeoutError.new(message, sql: sql, binds: binds)
        elsif exception.message.match(ERR_CONNECTION_FAILED_REGEX) || exception.message =~ ERR_CONNECTION_FAILED_MESSAGE
          begin
            reconnect!
            ::ODBCAdapter::ConnectionFailedError.new(message, sql: sql, binds: binds)
          rescue => e
            puts "unable to reconnect #{e}"
          end
        else
          super
        end
      end

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
