module ActiveRecord
  module Tasks
    class ODBCDatabaseTasks
      delegate :connection, :establish_connection, to: ActiveRecord::Base

      def initialize(configuration)
        @configuration = configuration
      end

      def create
        connection.create_database configuration['database']
        establish_connection configuration

      rescue ActiveRecord::StatementInvalid => error
        if /Database .* already exists/ === error.message
          raise DatabaseAlreadyExists
        else
          raise
        end
      end

      ActiveRecord::Tasks::DatabaseTasks.register_task(/odbc/, ActiveRecord::Tasks::ODBCDatabaseTasks)
    end
  end
end