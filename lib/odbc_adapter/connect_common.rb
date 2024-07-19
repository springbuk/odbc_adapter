# frozen_string_literal: true

require 'odbc'
require 'odbc_utf8'
require 'aws_secrets_manager'

module ODBCAdapter
  # Common support for establishing a connection or reconnecting to a database.
  class ConnectCommon
    class << self
      # Connect using a predefined DSN.
      def odbc_dsn_connection(config)
        username   = config[:username]&.to_s
        password   = config[:password]&.to_s
        odbc_module = config[:encoding] == 'utf8' ? ODBC_UTF8 : ODBC
        connection = odbc_module.connect(config[:dsn], username, password)

        # encoding_bug indicates that the driver is using non ASCII and has the issue referenced here https://github.com/larskanis/ruby-odbc/issues/2
        [connection, config.merge(username: username, password: password, encoding_bug: config[:encoding] == 'utf8')]
      end

      # Connect using an ODBC connection string.
      # Supports DSN-based or DSN-less connections
      # e.g. "DSN=virt5;UID=rails;PWD=rails"
      #      "DRIVER={OpenLink Virtuoso};HOST=carlmbp;UID=rails;PWD=rails"
      def odbc_conn_str_connection(config)
        attrs = config[:conn_str].split(';').map { |option| option.split('=', 2) }.to_h
        odbc_module = attrs['ENCODING'] == 'utf8' ? ODBC_UTF8 : ODBC

        # The connection string may specify an AWS secret key id as the value of PRIV_KEY_FILE. Development environmnets typically just use a filepath of a static key file.
        aws_secret_id = attrs['PRIV_KEY_FILE']&.start_with?(Rails.root.to_s) ? nil : attrs['PRIV_KEY_FILE']

        # when called from reconnect a driver may already be defined
        driver = config[:driver] || odbc_module::Driver.new

        if config[:driver]
          Rails.logger.info "odbc_adapter: Reconnecting using existing driver (#{driver.name})"
        else
          driver.name = 'odbc'
          driver.attrs = attrs
          AwsSecretsManager.configure_driver(driver, aws_secret_id) if aws_secret_id
        end

        begin
          Rails.logger.debug "odbc_adapter: Connecting with key file: #{driver.attrs['PRIV_KEY_FILE']}"
          connection = odbc_module::Database.new.drvconnect(driver)
        rescue odbc_module::Error => e
          # If the connection string specifies an AWS secret key id as the value of PRIV_KEY_FILE (instead of a filepath as used in development environments)
          # then attempt to fetch the latest private key file from AWS, serialize it and attempt to connect again. Local files are identified by a value starting with Rails.root
          # (such as '/path/to/private_key.pem')
          raise unless aws_secret_id && e.message.include?('private key')

          begin
            AwsSecretsManager.refresh_key_file(aws_secret_id)
          rescue AwsSecretsManager::AwsError => e
            raise ActiveRecord::DatabaseConnectionError, "Unable to determine correct database credentials from AWS secret: #{e.message}"
          else
            Rails.logger.info 'odbc_adapter: Attempting reconnect after refresh of key file'
            connection = odbc_module::Database.new.drvconnect(driver)
          end
        end

        # encoding_bug indicates that the driver is using non ASCII and has the issue referenced here https://github.com/larskanis/ruby-odbc/issues/2
        [connection, config.merge(driver: driver, encoding: attrs['ENCODING'], encoding_bug: attrs['ENCODING'] == 'utf8')]
      end
    end
  end
end
