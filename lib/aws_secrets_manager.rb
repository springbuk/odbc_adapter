# frozen_string_literal: true
require 'aws-sdk-secretsmanager'

class AwsSecretsManager
  @@cache_lock = Mutex.new
  KEY_FILE_LOCAL_NAME = 'aws_snowflake.pem'.freeze

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
      # return if Rails.env.development?

      # Avoid multiple workers atempting to fetch the same key file password from the AWS Secrets Manager
      @@cache_lock.synchronize do
        if force || !File.exist?(Rails.root.join(KEY_FILE_LOCAL_NAME).to_s)
          file_contents = fetch_aws_secret(aws_secret_id, force: force)
          # Write to the file even if the contents are blank to ensure we don't keep trying to fetch the key file unless another forced attempt is made
          File.write(Rails.root.join(KEY_FILE_LOCAL_NAME).to_s, file_contents)
        end
      end
    end

    def fetch_aws_secret(aws_secret_id, force: false)
      # Rails.logger.debug "Refreshing AWS key file (id: #{aws_secret_id}, force: #{force}, region: #{ENV.fetch("AWS_REGION", nil)})"
      puts "Refreshing AWS key file (id: #{aws_secret_id}, force: #{force}, region: #{ENV.fetch("AWS_REGION", nil)})"

      aws_exceptions = [Aws::SecretsManager::Errors::AccessDeniedException, Aws::SecretsManager::Errors::DecryptionFailure,
                        Aws::SecretsManager::Errors::InternalServiceError, Aws::SecretsManager::Errors::InvalidParameterException,
                        Aws::SecretsManager::Errors::InvalidRequestException, Aws::SecretsManager::Errors::ResourceNotFoundException]

      client = Aws::SecretsManager::Client.new(region: ENV.fetch("AWS_REGION", nil))
      begin
        secret_value_response = client.get_secret_value(secret_id: aws_secret_id)
      rescue Seahorse::Client::NetworkingError => e
        # occurs when client cannot connect to AWS, possibly a bad region
        puts "=====> Error retrieving AWS key file from Secrets Manager: #{e.message}"
        raise
      rescue *aws_exceptions => e
        # occurs when secret_id is not found or user does not have permission to access it
        puts "=====> Error retrieving AWS key file from Secrets Manager: #{e.message}"
        raise
      else
        secret_value_response.secret_string
      end
    end
  end
end
