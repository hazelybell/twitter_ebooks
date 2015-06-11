module Ebooks
  # This generator uses data identical to a markov model, but
  # instead of making a chain by looking up bigrams it uses the
  # positions to randomly replace suffixes in one sentence with
  # matching suffixes in another
  class LMDBBackedArray
    include Enumerable
    
    SUBARRAY_MAGIC = 1
    PACKED_MAGIC = 3
    
    def initialize(lmdbdb, prefix=[])
      @lmdbdb = lmdbdb
      @prefix = prefix
      @mykey = MessagePack.pack(@prefix)
      @size = nil
      @cache = {}
      
      asize = 0
      v = @lmdbdb.get(@mykey)
      if v == nil then
        asize = 0
      else
        (magic1, asize) = MessagePack.unpack(v)
        raise "Bad magic!" if (magic1 != SUBARRAY_MAGIC)
      end
      @size = asize
      
      self
    end
    
    def size
      return @size
    end
    
    def length
      return @size
    end
    
    def getv(v, key)
      return nil if v.nil?
      (magic1, rest) = MessagePack.unpack(v)
      if (magic1 == SUBARRAY_MAGIC)
        v = LMDBBackedArray.new(@lmdbdb, @prefix + [key])
        @cache[key] = v
        return v
      elsif (magic1 == PACKED_MAGIC)
#         @cache[key] = rest
        return rest
      else
        raise "Bad magic!"
      end
    end
    
    def get(key)
      return @cache[key] unless @cache[key].nil?
      binkey = MessagePack.pack(@prefix + [key])
      return getv(@lmdbdb.get(binkey), key)
    end
    
    def [](key)
      get(key)
    end
    
    def setsize(newsize)
      if newsize > 0 then
        @lmdbdb.put(@mykey, MessagePack.pack([SUBARRAY_MAGIC, newsize]))
      else
        begin
          @lmdbdb.delete(@mykey)
        rescue LMDB::Error::NOTFOUND
        end
      end
      @size = newsize
    end
    
    def delete(key)
      binkey = MessagePack.pack(@prefix + [key])
      @lmdbdb.delete(binkey)
      if (key == size()-1) then
        setsize(key)
      end
      @cache[key] = nil
    end
    
    def put(key, value)
      binkey = MessagePack.pack(@prefix + [key])
      if key > size-1
        setsize(key+1)
      end
      if value == []
        @lmdbdb.put(binkey, MessagePack.pack([SUBARRAY_MAGIC, 0]))
        value = LMDBBackedArray.new(@lmdbdb, @prefix + [key])
        @cache[key] = value
      elsif value == nil
        delete(key)
        @cache.delete(key)
      else
#         puts "#{value}"
        @lmdbdb.put(binkey, MessagePack.pack([PACKED_MAGIC, value]))
#         @lmdbdb.put(binkey, QARRAY_MAGIC.chr + value.pack('Q*'))
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
    
    def each
      startkey = MessagePack.pack(@prefix + [0])
      @lmdbdb.cursor do |cursor|
        (k, v) = cursor.set(startkey)
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
          yield(value) unless value.nil?
          lastindex = index
          (k, v) = cursor.next
        end
#         puts "each: #{lastindex} #{@size}"
        if lastindex < (@size-1) then
          for i in (lastindex+1)..(size-1) do
            item = get(i)
            raise "cursor didn't get them all!" unless item.nil?
            yield(item) unless item.nil?
          end
        end
      end
    end
    
    def append(o) 
      self[size] = o
      setsize(size+1)
    end
    
    def <<(o)
      append(o)
    end
    
    def cachereset
      @cache = {}
    end
  end
end