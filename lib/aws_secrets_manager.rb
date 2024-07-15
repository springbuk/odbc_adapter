# frozen_string_literal: true
require 'aws-sdk-secretsmanager'

class AwsSecretsManager
  @@cache_lock = Mutex.new
  KEY_FILE_LOCAL_NAME = 'aws_snowflake.pem'.freeze

  class AwsError < StandardError
  end

  class << self
    def configure_driver(driver, aws_secret_id)
      retrieve_key_file(aws_secret_id)

      driver.attrs['PRIV_KEY_FILE'] = Rails.root.join(AwsSecretsManager::KEY_FILE_LOCAL_NAME).to_s
    end

    def refresh_key_file(aws_secret_id)
      retrieve_key_file(aws_secret_id, force: true)
    end

    private

    def retrieve_key_file(aws_secret_id, force: false)
      # Avoid multiple workers atempting to fetch the same key file password from the AWS Secrets Manager
      @@cache_lock.synchronize do
        if force || !File.exist?(Rails.root.join(KEY_FILE_LOCAL_NAME).to_s)
          Rails.logger.info "AwsSecretsManager: Database key file #{force ? 'forced refresh' : 'not found'}, fetching from AWS"

          file_contents = fetch_aws_secret(aws_secret_id)
          File.write(Rails.root.join(KEY_FILE_LOCAL_NAME).to_s, file_contents)
        end
      end
    end

    def fetch_aws_secret(aws_secret_id)
      Rails.logger.debug "AwsSecretsManager: Retrieving AWS key file (id: #{aws_secret_id}, region: #{ENV.fetch('AWS_REGION', nil)})"

      # Handle all general AWS Secrets Manager exceptions. NOTE: AccessDeniedException isn't part of this collection and must be handled separately
      secret_exceptions = Aws::SecretsManager::Errors.constants.map do |e|
        Aws::SecretsManager::Errors.const_get(e)
      end.select { |e| e.is_a?(Class) && e < Exception }

      client = Aws::SecretsManager::Client.new(region: ENV.fetch('AWS_REGION', nil))
      begin
        secret_value_response = client.get_secret_value(secret_id: aws_secret_id)
      rescue Seahorse::Client::NetworkingError => e
        # Client cannot connect to AWS, possibly a bad region
        raise AwsError, e.message
      rescue Aws::SecretsManager::Errors::AccessDeniedException => e
        # User does not have permission to execute get_secret_value on the specified secret, most likely an incorrect secret_id
        raise AwsError, e.message
      rescue *secret_exceptions => e
        # All general AWS Secrets Manager exceptions
        raise AwsError, e.message
      else
        secret_value_response.secret_string
      end
    end
  end
end
