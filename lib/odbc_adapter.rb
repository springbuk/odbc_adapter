# Requiring with this pattern to mirror ActiveRecord
require 'active_record/connection_adapters/odbc_adapter'
require 'active_record/merge_all_persistence'

ActiveRecord::ConnectionAdapters.register("null_odbc", "ODBCAdapter::Adapters::NullODBCAdapter", "odbc_adapter/adapters/null_odbc_adapter.rb")
ActiveRecord::ConnectionAdapters.register("mysql_odbc", "ODBCAdapter::Adapters::MySQLODBCAdapter", "odbc_adapter/adapters/mysql_odbc_adapter.rb")
ActiveRecord::ConnectionAdapters.register("postgresql_odbc", "ODBCAdapter::Adapters::PostgreSQLODBCAdapter", "odbc_adapter/adapters/postgresql_odbc_adapter.rb")
ActiveRecord::ConnectionAdapters.register("snowflake_odbc", "ODBCAdapter::Adapters::SnowflakeODBCAdapter", "odbc_adapter/adapters/snowflake_odbc_adapter.rb")
