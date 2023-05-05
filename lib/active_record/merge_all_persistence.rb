require 'active_record/merge_all'

module ActiveRecord
  # = Active Record \Persistence
  module MergeAllPersistence
    extend ActiveSupport::Concern

    module ClassMethods
      def merge_all!(attributes, perform_inserts: true, perform_updates: true, delete_keys: [], prune_duplicates: false)
        MergeAll.new(self, attributes, perform_inserts: perform_inserts, perform_updates: perform_updates, delete_keys: delete_keys, prune_duplicates: prune_duplicates).execute
      end
    end
  end
end
