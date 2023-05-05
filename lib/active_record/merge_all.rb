# frozen_string_literal: true

require "active_support/core_ext/enumerable"

module ActiveRecord
  class MergeAll # :nodoc:
    attr_reader :model, :connection, :merges, :keys
    attr_reader :perform_inserts, :perform_updates, :delete_keys

    def initialize(model, merges, perform_inserts: true, perform_updates: true, delete_keys: [], prune_duplicates: false)
      raise ArgumentError, "Empty list of attributes passed" if merges.blank?

      @model, @connection, @merges, @keys = model, model.connection, merges, merges.first.keys.map(&:to_s)
      @perform_inserts, @perform_updates, @delete_keys = perform_inserts, perform_updates, delete_keys.map(&:to_s)

      if model.scope_attributes?
        @scope_attributes = model.scope_attributes
        @keys |= @scope_attributes.keys
      end
      @keys = @keys.to_set

      ensure_valid_options_for_connection!

      if prune_duplicates
        do_prune_duplicates
      end
    end

    def execute
      message = +"#{model} "
      message << "Bulk " if merges.many?
      message << "Merge"
      connection.exec_merge_all to_sql, message
    end

    def updatable_columns
      keys - readonly_columns - delete_keys
    end

    def insertable_columns
      keys - delete_keys
    end

    def insertable_non_primary_columns
      insertable_columns - primary_keys
    end

    def primary_keys
      Array(connection.schema_cache.primary_keys(model.table_name))
    end

    def map_key_with_value
      merges.map do |attributes|
        attributes = attributes.stringify_keys
        attributes.merge!(scope_attributes) if scope_attributes

        verify_attributes(attributes)

        keys.map do |key|
          yield key, attributes[key]
        end
      end
    end

    def perform_deletes
      !delete_keys.empty?
    end

    private
    attr_reader :scope_attributes

    def ensure_valid_options_for_connection!

    end

    def do_prune_duplicates
      unless primary_keys.to_set.subset?(keys)
        raise ArgumentError, "Pruning duplicates requires presense of all primary keys in the merges"
      end
      @merges = merges.reverse.uniq do |merge|
        primary_keys.map { |key| merge[key] }
      end.reverse
    end

    def to_sql
      connection.build_merge_sql(ActiveRecord::MergeAll::Builder.new(self))
    end

    def readonly_columns
      primary_keys + model.readonly_attributes.to_a
    end

    def verify_attributes(attributes)
      if keys != attributes.keys.to_set
        raise ArgumentError, "All objects being merged must have the same keys"
      end
    end

    class Builder # :nodoc:
      attr_reader :model

      delegate :keys, to: :merge_all

      def initialize(merge_all)
        @merge_all, @model, @connection = merge_all, merge_all.model, merge_all.connection
      end

      def into
        # "INTO #{model.quoted_table_name} (#{columns_list})"
        "INTO #{model.quoted_table_name}"
      end

      def values_list
        types = extract_types_from_columns_on(model.table_name, keys: keys)

        values_list = merge_all.map_key_with_value do |key, value|
          connection.with_yaml_fallback(types[key].serialize(value))
        end

        values = connection.visitor.compile(Arel::Nodes::ValuesList.new(values_list))

        "SELECT * FROM (#{values}) AS v1 (#{columns_list})"
      end

      def match
        quote_columns(merge_all.primary_keys).map { |column| "SOURCE.#{column}=TARGET.#{column}" }.join(" AND ")
      end

      def merge_delete
        merge_all.perform_deletes ? "WHEN MATCHED AND #{quote_columns(merge_all.delete_keys).map { |column| "SOURCE.#{column} = TRUE"}.join(" AND ")} THEN DELETE" : ""
      end

      def merge_update
        merge_all.perform_updates ? "WHEN MATCHED THEN UPDATE SET #{updatable_columns.map { |column| "TARGET.#{column}=SOURCE.#{column}" }.join(",")}" : ""
      end

      def merge_insert
        if merge_all.perform_inserts
          <<~SQL
            WHEN NOT MATCHED AND #{quote_columns(merge_all.primary_keys).map { |column| "SOURCE.#{column} IS NOT NULL" }.join(" AND ")} THEN INSERT (#{insertable_columns_list}) VALUES (#{quote_columns(merge_all.insertable_columns).map { |column| "SOURCE.#{column}"}.join(",")})
            WHEN NOT MATCHED AND #{quote_columns(merge_all.primary_keys).map { |column| "SOURCE.#{column} IS NULL" }.join(" OR ")} THEN INSERT (#{insertable_non_primary_columns_list}) VALUES (#{quote_columns(merge_all.insertable_non_primary_columns).map { |column| "SOURCE.#{column}"}.join(",")})
          SQL
        else
          ""
        end
      end

      private
      attr_reader :connection, :merge_all

      def columns_list
        format_columns(merge_all.keys)
      end

      def insertable_columns_list
        format_columns(merge_all.insertable_columns)
      end

      def insertable_non_primary_columns_list
        format_columns(merge_all.insertable_non_primary_columns)
      end

      def updatable_columns
        quote_columns(merge_all.updatable_columns)
      end

      def extract_types_from_columns_on(table_name, keys:)
        columns = connection.schema_cache.columns_hash(table_name)

        unknown_column = (keys - columns.keys).first
        raise UnknownAttributeError.new(model.new, unknown_column) if unknown_column

        keys.index_with { |key| model.type_for_attribute(key) }
      end

      def format_columns(columns)
        columns.respond_to?(:map) ? quote_columns(columns).join(",") : columns
      end

      def quote_columns(columns)
        columns.map(&connection.method(:quote_column_name))
      end
    end
  end
end