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
    
    def get(key)
      binkey = MessagePack.pack(@prefix + [key])
      v = @lmdbdb.get(binkey)
      return nil if v.nil?
      (magic1, rest) = MessagePack.unpack(v)
      if (magic1 == SUBARRAY_MAGIC)
        return LMDBBackedArray.new(@lmdbdb, @prefix + [key])
      elsif (magic1 == PACKED_MAGIC)
        return rest
      else
        raise "Bad magic!"
      end
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
    end
    
    def put(key, value)
      binkey = MessagePack.pack(@prefix + [key])
      if key > size-1
        setsize(key+1)
      end
      if value == []
        @lmdbdb.put(binkey, MessagePack.pack([SUBARRAY_MAGIC, 0]))
        value = LMDBBackedArray.new(@lmdbdb, @prefix + [key])
      elsif value == nil
        delete(key)
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
      for i in 0..(size-1) do
        yield(get(i))
      end
    end
    
    def append(o) 
      self[size] = o
      setsize(size+1)
    end
    
    def <<(o)
      append(o)
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
      FileUtils.mkdir_p(dbdir)
      @env = LMDB.new dbdir, :writemap => true, :mapasync => true, :nosync => true, :nometasync => true
      @sentences = LMDBBackedArray.new(@env.database("sentences", {:create => true}))
      @unigrams = LMDBBackedArray.new(@env.database("unigrams", {:create => true}))
      @bigrams = LMDBBackedArray.new(@env.database("bigrams", {:create => true}))
      sentences = sentences.reject{ |s| s.length < 2 }
      if @sentences.size > sentences.size then
        @sentences.clear
        @unigrams.clear
        @bigrams.clear
      end
      if @sentences.size < sentences.size then
        ii = 0
        i = 0
        while (ii < sentences.size)
          begin
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
            @env.mapsize=(((@env.info[:mapsize]*1.4)/(1024*1024)).ceil * (1024*1024))
            retry
          end
          ii=i
        end
      end

      self
    end


    # Generate a recombined sequence of tikis
    # @param passes [Integer] number of times to recombine
    # @param n [Symbol] :unigrams or :bigrams (affects how conservative the model is)
    # @return [Array<Integer>]
    def generate(passes=5, n=:unigrams)
      index = rand(@sentences.length)
      tikis = @sentences[index]
      used = [index] # Sentences we've already used
      verbatim = [tikis] # Verbatim sentences to avoid reproducing

      (0..passes).each do |passno|
        puts "Generating... pass ##{passno}"
        varsites = {} # Map bigram start site => next tiki alternatives

        tikis.each_with_index do |tiki, i|
          next_tiki = tikis[i+1]
          break if next_tiki.nil?

          alternatives = (n == :unigrams) ? @unigrams[next_tiki] : @bigrams[tiki][next_tiki]
          # Filter out suffixes from previous sentences
          alternatives.reject! { |a| a[1] == INTERIM || used.include?(a[0]) }
          varsites[i] = alternatives unless alternatives.empty?
        end

        variant = nil
        ia = 0
        varsites.to_a.shuffle.each do |site|
          
          start = site[0]
          ib = 0
          site[1].shuffle.each do |alt|
            puts "Variant #{ia} site #{ib}" if (ib % 10000) == 0
            ib += 1
            verbatim << @sentences[alt[0]]
            suffix = @sentences[alt[0]][alt[1]..-1]
            potential = tikis[0..start+1] + suffix

            # Ensure we're not just rebuilding some segment of another sentence
            unless verbatim.find { |v| NLP.subseq?(v, potential) || NLP.subseq?(potential, v) }
              used << alt[0]
              variant = potential
              break
            end
            return nil if ib > 100000 # got stuck. still don't know what causes this...
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
