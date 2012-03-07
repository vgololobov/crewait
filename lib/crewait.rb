#encoding: UTF-8
module Crewait

  def self.start_waiting(config = {})
    @@config ||= {}
    @@config.merge!(config)
    @@hash_of_hashes, @@hash_of_next_inserts = {},{}
  end


  def self.for(model, hash)
    # if this class is new, add in the next_insert_value
    @@hash_of_next_inserts[model] ||= model.next_insert_id
    # if this class is new, create a new hash to receive it
    @@hash_of_hashes[model] ||= {}
    @@hash_of_hashes[model].respectively_insert(hash)
    hash[:id] = @@hash_of_next_inserts[model] + @@hash_of_hashes[model].inner_length - 1

    unless @@config[:no_methods]
      eigenclass = class << hash; self; end
      eigenclass.class_eval { hash.each {|key, value| define_method(key) { value } }}
    end
    hash
  end

  def self.go!
    @@hash_of_hashes.each {|key, hash| hash.import_to_sql(key)}
    @@hash_of_hashes,@@hash_of_next_inserts = {},{}
  end

  module BaseMethods
    def next_insert_id
      #todo add back adapter select
      table,database = self.table_name, ActiveRecord::Base.connection.current_database
      sql = "SELECT auto_increment FROM information_schema.tables WHERE table_name='#{table}' AND table_schema ='#{database}'"
      results = ActiveRecord::Base.connection.execute(sql)
      results[0]['nextval'].to_i
    end

    def crewait(hash)
			Crewait.for(self, hash)
  	end
  end

  module HashMethods
    def import_to_sql(model_class)

      model_class = model_class.table_name if model_class.respond_to? :table_name
      keys, values = self.keys , []
      keys.each  {|key| values << ( self[key].any?(&:!) ? self[key] : self[key].map{|x| 1} )}

      values = values.transpose
      sql = values.to_crewait_sql

  		while !sql.empty? do
  			query_string = "INSERT INTO #{model_class} (`#{keys.join("`, `")}`) VALUES #{sql.shift}"
  			while !sql.empty? && (query_string.length + sql.last.length < 999999)  do
  				query_string << ',' << sql.shift
  			end
        ActiveRecord::Base.connection.execute(query_string)
  		end
    end

    def respectively_insert(other_hash)

      new_keys = other_hash.keys - self.keys
      length = new_keys.empty? ? 0 : self.inner_length

      new_keys.each { |key| self[key] = Array.new(length) }
      self.keys.each { |key|  self[key] << other_hash[key]}
    end

    def inner_length
      self.values.empty? ? 0 : self.values.first.length
    end
  end

  module ArrayMethods
    def to_crewait_sql
    	self.map {|x| "(#{x.map{|y| y.nil? ? 'NULL' : ActiveRecord::Base.sanitize(y)}.join(', ')})" }
    end
  end
end

class ActiveRecord::Base
  extend Crewait::BaseMethods
end

class Hash
  include Crewait::HashMethods
end

class Array
  include Crewait::ArrayMethods
end
