# encoding: utf-8
require 'fileutils'
require 'lmdb'
require 'msgpack'

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
  
  class SuffixGenerator
    # Build a generator from a corpus of tikified sentences
    # @param sentences [Array<Array<Integer>>]
    # @return [SuffixGenerator]
    def self.build(name, sentences)
      SuffixGenerator.new(name, sentences)
    end

    def initialize(name, sentences)
      dbdir = File.join("model", name)
      puts "Using DB: #{dbdir}"
      FileUtils.mkdir_p(dbdir)
      @env = LMDB.new dbdir, :nometasync => true, :mapasync => true, :nosync => true
      @sentences = LMDBBackedArray.new(@env.database("sentences", {:create => true}))
      @unigrams = LMDBBackedArray.new(@env.database("unigrams", {:create => true}))
      @bigrams = LMDBBackedArray.new(@env.database("bigrams", {:create => true}))
      sentences = sentences.reject{ |s| s.length < 2 }
      if @sentences.size > sentences.size then
        raise "Sentences shrunk!"
#         @sentences.clear
#         @unigrams.clear
#         @bigrams.clear
      end
      if @sentences.size < sentences.size then
        ii = 0
        i = 0
        while (ii < sentences.size)
          begin
            @sentences.cachereset
            @unigrams.cachereset
            @bigrams.cachereset
            @env.transaction do |trans|
              i = ii
              s = ii+1000
              log ("Building: sentence #{i} of #{sentences.length}")
              while (i < s && i < sentences.size) 
                tikis = sentences[i]
                if @sentences[i].nil? then
                  @sentences[i] = tikis
                  last_tiki = INTERIM
                  tikis.each_with_index do |tiki, j|
                    @unigrams[last_tiki] ||= []
                    @unigrams[last_tiki] << [i, j]

                    @bigrams[last_tiki] ||= []
                    @bigrams[last_tiki][tiki] ||= []

                    if j == tikis.length-1 # Mark sentence endings
                      @unigrams[tiki] ||= []
                      @unigrams[tiki] << [i, INTERIM]
                      @bigrams[last_tiki][tiki] << [i, INTERIM]
                    else
                      @bigrams[last_tiki][tiki] << [i, j+1]
                    end

                    last_tiki = tiki
                  end
                else
                  unless @sentences[i] == tikis then
                    raise "Data bad/corrput?"
                  end
                end
                i += 1
              end
            end
          rescue LMDB::Error::MAP_FULL
            previousMapsize = @env.info[:mapsize]
            newMapsize = previousMapsize * 1.4
            realnewMapsize = (newMapsize/(1024*1024)).ceil * 1024 * 1024
            puts "Previous map size: #{previousMapsize} new #{newMapsize} rounded #{realnewMapsize}"
            @env.mapsize=(realnewMapsize)
            retry
          end
          ii=i
        end
      end

      self
    end
    
    def self.subseq?(a1, a2)
      return (a1 == a2) if a1.length == a2.length
      return true if a1.length == 0
      return true if a2.length == 0
      a1,a2 = a2,a1 if a2.length > a1.length # a2 is now the shorter
      start = a1.index(a2[0])
      return false if start.nil?
      return (a1[start...(start+a2.length-1)] == a2)
    end



    # Generate a recombined sequence of tikis
    # @param passes [Integer] number of times to recombine
    # @param n [Symbol] :unigrams or :bigrams (affects how conservative the model is)
    # @return [Array<Integer>]
    def generate(passes=5, n=:unigrams)
      unigramsOn = (n == :unigrams)
      index = rand(@sentences.length)
      tikis = @sentences[index]
      used = [index] # Sentences we've already used
      verbatim = [tikis] # Verbatim sentences to avoid reproducing

      (1..passes).each do |passno|
        log "Generating... pass ##{passno}/#{passes}"
        varsites = {} # Map bigram start site => next tiki alternatives

        tikis.each_with_index do |tiki, i|
          next_tiki = tikis[i+1]
          next if i == 0
          break if next_tiki.nil?

          alternatives = unigramsOn ? @unigrams[next_tiki] : @bigrams[tiki][next_tiki]
          # Filter out suffixes from previous sentences
          alternatives = alternatives.reject { |a| a[1] == INTERIM || used.include?(a[0]) }
          alternatives = alternatives.sample(10000)
          varsites[i] = alternatives unless alternatives.empty?
        end

        variant = nil
        ia = 0
        varsites.to_a.shuffle.each do |site|
          
          start = site[0]
          ib = 0
          site[1].each do |alt|
            puts "Site #{start}/#{varsites.length} alt #{ib}/#{site[1].length}" if (ib % 1000) == 0
            ib += 1
            alts = @sentences[alt[0]]
            verbatim << alts
            suffix = alts[alt[1]..-1]
            puts "Zero length!" if suffix.length < 1
            potential = tikis[0..start+1] + suffix

            # Ensure we're not just rebuilding some segment of another sentence
            unless verbatim.find { |v| v.length > 1 && SuffixGenerator.subseq?(v, potential) }
              used << alt[0]
              variant = potential
              break
            end
            raise("Wargh") if ib > 100000 # got stuck. still don't know what causes this...
          end
          ia += 1
          break if variant
        end

        tikis = variant if variant
      end

      tikis
    end
  end
end
