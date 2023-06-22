module ODBCAdapter
  module SchemaStatements
    # Returns a Hash of mappings from the abstract data types to the native
    # database types. See TableDefinition#column for details on the recognized
    # abstract data types.
    def native_database_types
      @native_database_types ||= ColumnMetadata.new(self).native_database_types
    end

    # Returns an array of table names, for database tables visible on the
    # current connection.
    def tables(_name = nil)
      stmt   = @connection.tables
      result = stmt.fetch_all || []
      stmt.drop

      db_regex = name_regex(current_database)
      schema_regex = name_regex(current_schema)
      result.each_with_object([]) do |row, table_names|
        next unless row[0] =~ db_regex && row[1] =~ schema_regex
        schema_name, table_name, table_type = row[1..3]
        next if respond_to?(:table_filtered?) && table_filtered?(schema_name, table_type)
        table_names << format_case(table_name)
      end
    end

    # Returns an array of view names defined in the database.
    def views
      []
    end

    # Returns an array of indexes for the given table.
    def indexes(table_name, _name = nil)
      stmt   = @connection.indexes(native_case(table_name.to_s))
      result = stmt.fetch_all || []
      stmt.drop unless stmt.nil?

      index_cols = []
      index_name = nil
      unique     = nil

      db_regex = name_regex(current_database)
      schema_regex = name_regex(current_schema)
      result.each_with_object([]).with_index do |(row, indices), row_idx|
        next unless row[0] =~ db_regex && row[1] =~ schema_regex
        # Skip table statistics
        next if row[6].zero? # SQLStatistics: TYPE

        if row[7] == 1 # SQLStatistics: ORDINAL_POSITION
          # Start of column descriptor block for next index
          index_cols = []
          unique     = row[3].zero? # SQLStatistics: NON_UNIQUE
          index_name = String.new(row[5]) # SQLStatistics: INDEX_NAME
        end

        index_cols << format_case(row[8]) # SQLStatistics: COLUMN_NAME
        next_row = result[row_idx + 1]

        if (row_idx == result.length - 1) || (next_row[6].zero? || next_row[7] == 1)
          indices << ActiveRecord::ConnectionAdapters::IndexDefinition.new(table_name, format_case(index_name), unique, index_cols)
        end
      end
    end

    def columns_test()
      tables_query = <<~SQL
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_CATALOG = CURRENT_DATABASE() AND TABLE_SCHEMA = CURRENT_SCHEMA()
      SQL
      table_list = exec_query(tables_query)
      ActiveRecord::Base.logger.debug("Retrieving columns for #{table_list.length} tables")
      table_list.map.with_index do |record, index|
        ActiveRecord::Base.logger.debug("Retrieving columns for #{record["table_name"]}, #{index+1} of #{table_list.length}")
        columns(record["table_name"])
      end
      nil
    end

    # Extracts the value from a Snowflake column default definition.
    def extract_value_from_default(default)
      case default
        # null
      when nil
        nil
        # Quoted strings
      when /\A[(B]?'(.*)'\z/m
        $1.gsub("''", "'").gsub("\\\\","\\")
        # Boolean types
      when "TRUE"
        puts "col_default: #{default.inspect}"
        "true"
      when "FALSE"
        puts "col_default: #{default.inspect}"
        "false"
        # Numeric types
      when /\A(-?\d+(\.\d*)?)\z/
        $1
      else
        nil
      end
    end

    def retrieve_column_data(table_name)
      if @table_store == nil || @table_store[table_name] == nil
        column_query = <<~SQL
          SELECT TABLE_CATALOG,
          TABLE_SCHEMA,
          TABLE_NAME,
          COLUMN_NAME,
          COLUMN_DEFAULT,
          CASE
            WHEN DATA_TYPE = 'NUMBER' THEN 'DECIMAL'
            WHEN DATA_TYPE like 'TIMESTAMP_%' THEN 'TIMESTAMP'
            WHEN DATA_TYPE = 'TEXT' THEN 'VARCHAR'
            WHEN DATA_TYPE = 'FLOAT' THEN 'DOUBLE'
            ELSE DATA_TYPE
          END AS DATA_TYPE,
          CASE
            WHEN DATA_TYPE like 'TIMESTAMP_%' THEN 35
            WHEN DATA_TYPE = 'DATE' THEN 10
            WHEN DATA_TYPE = 'FLOAT' THEN 38
            WHEN DATA_TYPE = 'BOOLEAN' THEN 1
            ELSE COALESCE(CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, 0)
          END AS COLUMN_SIZE,
          CASE
            WHEN DATA_TYPE like 'TIMESTAMP_%' THEN DATETIME_PRECISION
            ELSE COALESCE(NUMERIC_SCALE, 0)
          END AS NUMERIC_SCALE,
          IS_NULLABLE = 'YES' AS IS_NULLABLE
          FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_CATALOG = CURRENT_DATABASE() AND TABLE_SCHEMA = CURRENT_SCHEMA() AND TABLE_NAME = #{quote(native_case(table_name))}
          ORDER BY ORDINAL_POSITION
        SQL

        logger = ActiveRecord::Base.logger.level
        ActiveRecord::Base.logger.level = Logger::WARN
        column_data ||= exec_query(column_query)
        ActiveRecord::Base.logger.level = logger

        @table_store = {}
        column_data.each do | column |
          @table_store[column["table_name"]] = [] unless @table_store.key?(column["table_name"])
          @table_store[column["table_name"]].push(column)
        end

        p "Retrieved initial store"
      end

      @table_store[native_case(table_name.to_s)]
    end


    # Returns an array of Column objects for the table specified by
    # +table_name+.
    # This entire function has been customized for Snowflake and will not work in general.
    def columns(table_name, _name = nil)
      result = retrieve_column_data(table_name)

      result.each_with_object([]) do |col, cols|
        col_name        = col["column_name"]
        col_default     = extract_value_from_default(col["column_default"])
        col_native_type = col["data_type"]
        col_limit       = col["column_size"]
        col_scale       = col["numeric_scale"]
        col_nullable    = col["is_nullable"]

        args = { sql_type: construct_sql_type(col_native_type, col_limit, col_scale), type: col_native_type, limit: col_limit }
        args[:type] = case col_native_type
                      when "BOOLEAN" then :boolean
                      when "VARIANT" then :variant
                      when "ARRAY" then :array
                      when "STRUCT" then :object
                      when "DATE" then :date
                      when "VARCHAR" then :string
                      when "TIMESTAMP" then :datetime
                      when "TIME" then :time
                      when "BINARY" then :binary
                      when "DOUBLE" then :float
                      when "DECIMAL"
                        if col_scale == 0
                          :integer
                        else
                          args[:scale]     = col_scale
                          args[:precision] = col_limit
                          :decimal
                        end
                      else
                        nil
                      end

        sql_type_metadata = ActiveRecord::ConnectionAdapters::SqlTypeMetadata.new(**args)

        # The @connection.columns function returns empty strings for column defaults.
        # Even when the column has a default value. This is a call to the ODBC layer
        # with only enough Ruby to make the call happen. Replacing the empty string
        # with nil permits Rails to set the current datetime for created_at and
        # updated_at on model creates and updates.
        col_default = nil if col_default == ""

        cols << new_column(format_case(col_name), col_default, sql_type_metadata, col_nullable, col_native_type)
      end
    end

    # Returns just a table's primary key
    def primary_key(table_name)
      stmt   = @connection.primary_keys(native_case(table_name.to_s))
      result = stmt.fetch_all || []
      stmt.drop unless stmt.nil?

      db_regex = name_regex(current_database)
      schema_regex = name_regex(current_schema)
      result.reduce(nil) { |pkey, key| (key[0] =~ db_regex && key[1] =~ schema_regex) ? format_case(key[3]) : pkey }
    end

    def foreign_keys(table_name)
      stmt   = @connection.foreign_keys(native_case(table_name.to_s))
      result = stmt.fetch_all || []
      stmt.drop unless stmt.nil?

      db_regex = name_regex(current_database)
      schema_regex = name_regex(current_schema)
      result.map do |key|
        next unless key[0] =~ db_regex && key[1] =~ schema_regex
        fk_from_table      = key[2]  # PKTABLE_NAME
        fk_to_table        = key[6]  # FKTABLE_NAME

        ActiveRecord::ConnectionAdapters::ForeignKeyDefinition.new(
          fk_from_table,
          fk_to_table,
          name:        key[11], # FK_NAME
          column:      key[3],  # PKCOLUMN_NAME
          primary_key: key[7],  # FKCOLUMN_NAME
          on_delete:   key[10], # DELETE_RULE
          on_update:   key[9]   # UPDATE_RULE
        )
      end
    end

    # Ensure it's shorter than the maximum identifier length for the current
    # dbms
    def index_name(table_name, options)
      maximum = database_metadata.max_identifier_len || 255
      super(table_name, options)[0...maximum]
    end

    def current_database
      database_metadata.database_name.strip
    end

    def current_schema
      @config[:driver].attrs['schema']
    end

    def name_regex(name)
      if name =~ /^".*"$/
        /^#{name.delete_prefix('"').delete_suffix('"')}$/
      else
        /^#{name}$/i
      end
    end

    # Changes in rails 7 mean that we need all of the type information in the sql_type column
    # This reconstructs sql types using limit (which is precision) and scale
    def construct_sql_type(native_type, limit, scale)
      if scale > 0
        "#{native_type}(#{limit},#{scale})"
      elsif limit > 0
        "#{native_type}(#{limit})"
      else
        native_type
      end
    end
  end
end
