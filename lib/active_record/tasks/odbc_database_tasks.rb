module ActiveRecord
  module Tasks
    class ODBCDatabaseTasks
      delegate :connection, :establish_connection, to: ActiveRecord::Base

      def initialize(configuration)
        @configuration = configuration
      end

      ActiveRecord::Tasks::DatabaseTasks.register_task(/odbc/, ActiveRecord::Tasks::ODBCDatabaseTasks)
    end
  end
end