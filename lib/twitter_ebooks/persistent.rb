module Ebooks
  SUBARRAY_MAGIC = 1
  SUBHASH_MAGIC = 2
  PACKED_MAGIC = 3
  
  class LMDBBackedHash
    include Enumerable
    
    def initialize(lmdbdb, prefix=[])
      @lmdbdb = lmdbdb
      @prefix = prefix
      @mykey = MessagePack.pack(@prefix)
      
      self
    end
    
    def env
      @lmdbdb.env
    end
    
    def size
      asize = 0
      v = @lmdbdb.get(@mykey)
      if v == nil then
        asize = 0
      else
        (magic1, asize) = MessagePack.unpack(v)
        raise "Bad magic!" if (magic1 != SUBHASH_MAGIC)
      end
      return asize
    end
    
    def length
      return size
    end
    
    def getv(v, key)
      return nil if v.nil?
      (magic1, rest) = MessagePack.unpack(v)
      if (magic1 == SUBARRAY_MAGIC)
        v = LMDBBackedArray.new(@lmdbdb, @prefix + [key])
        return v
      elsif (magic1 == SUBHASH_MAGIC)
        v = LMDBBackedHash.new(@lmdbdb, @prefix + [key])
        return v
      elsif (magic1 == PACKED_MAGIC)
        return rest
      else
        raise "Bad magic!"
      end
    end
    
    def get(key)
      binkey = MessagePack.pack(@prefix + [key])
      return getv(@lmdbdb.get(binkey), key)
    end
    
    def has_key?(key)
      binkey = MessagePack.pack(@prefix + [key])
      return true if @lmdbdb.get(binkey)
    end
    
    def [](key)
      get(key)
    end
    
    def setsize(newsize)
      if newsize > 0 then
        @lmdbdb.put(@mykey, MessagePack.pack([SUBHASH_MAGIC, newsize]))
      else
        begin
          @lmdbdb.delete(@mykey)
        rescue LMDB::Error::NOTFOUND
        end
      end
    end
    
    def delete(key)
      binkey = MessagePack.pack(@prefix + [key])
      old = get(key)
      unless old.nil? # not nil..
        old.clear if old.is_a?(LMDBBackedHash)
        @lmdbdb.delete(binkey)
        setsize(size-1)
      end
    end
    
    def put(key, value)
      binkey = MessagePack.pack(@prefix + [key])
      old = get(key)
      if old.nil?
        setsize(size+1) # For hashes
      else # not nil..
        old.clear if old.is_a?(LMDBBackedHash)
      end
      if value == []
        @lmdbdb.put(binkey, MessagePack.pack([SUBARRAY_MAGIC, 0]))
        value = LMDBBackedArray.new(@lmdbdb, @prefix + [key])
      elsif value == {}
        @lmdbdb.put(binkey, MessagePack.pack([SUBHASH_MAGIC, 0]))
        value = LMDBBackedHash.new(@lmdbdb, @prefix + [key])
      elsif value == nil
        delete(key)
      else
        @lmdbdb.put(binkey, MessagePack.pack([PACKED_MAGIC, value]))
      end
      value
    end
    
    def []=(key, value)
      put(key, value)
    end
    
    def clear
      if (@prefix.length > 0) then
        for i in 0..(size-1) do
          delete(i)
        end
        setsize(0)
      else
        @lmdbdb.clear
      end
    end
    
    def each_pair
      startkey = MessagePack.pack(@prefix + [0])
      count = 0
      @lmdbdb.cursor do |cursor|
        k, v = nil, nil
        begin
          (k, v) = cursor.set(startkey)
        rescue LMDB::Error::NOTFOUND
          begin
            (k, v) = cursor.set_range(startkey)
          rescue LMDB::Error::NOTFOUND
            return
          end
        end
        stop = false
        lastindex = 0
        until stop do
          if k.nil?
            break
          end
          k = MessagePack.unpack(k)
          index = k.pop
          if k != @prefix
#             puts "#{k}!=#{@prefix} #{index}" 
            break
          end
          count += 1
          value = getv(v, index)
          yield(index, value) unless value.nil? # each_pair is backwards of each_with_index
          lastindex = index
          (k, v) = cursor.next
        end
        if count < (size-1) then
          raise "cursor didn't get them all!" unless item.nil?
        end
      end
    end
    
    def each
      each_pair do |key, value|
        yield(value)
      end
    end
    
    def each_key
      each_with_index do |key, value|
        yield(key)
      end
    end
    
    def expand()
      previousMapsize = env.info[:mapsize]
      newMapsize = previousMapsize * 1.4
      realnewMapsize = (newMapsize/(1024*1024)).ceil * 1024 * 1024
      puts "Previous map size: #{previousMapsize} new #{newMapsize} rounded #{realnewMapsize}"
      env.mapsize=(realnewMapsize)
    end
  end

  # -------------------------------------------------------------------------------------------------------
  class LMDBBackedArray < LMDBBackedHash
    include Enumerable
    
    def size
      asize = 0
      v = @lmdbdb.get(@mykey)
      if v == nil then
        asize = 0
      else
        (magic1, asize) = MessagePack.unpack(v)
        raise "Bad magic!" if (magic1 != SUBARRAY_MAGIC)
      end
      raise "wat!" unless asize.is_a?(Integer)
      return asize
    end
    
    def setsize(newsize)
      raise "wat!" unless newsize.is_a?(Integer)
      if newsize > 0 then
        @lmdbdb.put(@mykey, MessagePack.pack([SUBARRAY_MAGIC, newsize]))
      else
        begin
          @lmdbdb.delete(@mykey)
        rescue LMDB::Error::NOTFOUND
        end
      end
    end
    
    def delete(key)
      binkey = MessagePack.pack(@prefix + [key])
      @lmdbdb.delete(binkey)
      if (key == size()-1) then
        setsize(key)
      end
    end
    
    def put(key, value)
      binkey = MessagePack.pack(@prefix + [key])
      if key >= size
        setsize(key+1)
      end
      old = get(key)
      old.clear if old.is_a?(LMDBBackedHash)
      if value == []
        @lmdbdb.put(binkey, MessagePack.pack([SUBARRAY_MAGIC, 0]))
        value = LMDBBackedArray.new(@lmdbdb, @prefix + [key])
      elsif value == {}
        @lmdbdb.put(binkey, MessagePack.pack([SUBHASH_MAGIC, 0]))
        value = LMDBBackedHash.new(@lmdbdb, @prefix + [key])
      elsif value == nil
        delete(key)
      else
        @lmdbdb.put(binkey, MessagePack.pack([PACKED_MAGIC, value]))
      end
      value
    end
    
    def []=(key, value)
      put(key, value)
    end
    
    def clear
      if (@prefix.length > 0) then
        for i in 0..(size-1) do
          delete(i)
        end
        setsize(0)
      else
        @lmdbdb.clear
      end
    end
    
    def each_with_index
      startkey = MessagePack.pack(@prefix + [0])
      @lmdbdb.cursor do |cursor|
        k, v = nil, nil
        begin
          (k, v) = cursor.set(startkey)
        rescue LMDB::Error::NOTFOUND
          begin
            (k, v) = cursor.set_range(startkey)
          rescue LMDB::Error::NOTFOUND
            return
          end
        end
        stop = false
        lastindex = 0
        until stop do
          if k.nil?
            break
          end
          k = MessagePack.unpack(k)
          index = k.pop
          if k != @prefix
#             puts "#{k}!=#{@prefix} #{index}" 
            break
          end
          raise "cursor decreasing :(" if index < lastindex
          value = getv(v, index)
          yield(value, index) unless value.nil?
          lastindex = index
          (k, v) = cursor.next
        end
#         puts "each: #{lastindex} #{@size}"
        if lastindex < (size-1) then
          for i in (lastindex+1)..(size-1) do
            item = get(i)
            raise "cursor didn't get them all!" unless item.nil?
            yield(item, i) unless item.nil?
          end
        end
      end
    end
    
    def each
      each_with_index do |value, index|
        yield(value)
      end
    end
    
    def each_index
      each_with_index do |value, index|
        yield(index)
      end
    end
    
    def append(o) 
      put(size, o)
    end
    
    def <<(o)
      append(o)
    end
    
    def import(a)
      i = 0
      @lmdbdb.clear
      while (i < a.size)
        begin
          put(i, a[i])
        rescue LMDB::Error::MAP_FULL
          expand
          retry
        end
        i+=1
      end
    end
  end
end