module ActiveRecord
  module Tasks
    class ODBCDatabaseTasks
      delegate :connection, :establish_connection, to: ActiveRecord::Base

      def initialize(configuration)
        p configuration
        @configuration = configuration
      end

      def create
        establish_connection configuration
        connection.create_database configuration['database']

      rescue ActiveRecord::StatementInvalid => error
        if /Database .* already exists/ === error.message
          raise DatabaseAlreadyExists
        else
          raise
        end
      end

      private

      def configuration
        @configuration
      end

      ActiveRecord::Tasks::DatabaseTasks.register_task(/odbc/, ActiveRecord::Tasks::ODBCDatabaseTasks)
    end
  end
end