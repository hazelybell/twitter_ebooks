# encoding: utf-8
require 'fileutils'
require 'lmdb'
require 'msgpack'

module Ebooks
  # This generator uses data identical to a markov model, but
  # instead of making a chain by looking up bigrams it uses the
  # positions to randomly replace suffixes in one sentence with
  # matching suffixes in another  
  class SuffixGenerator
    # Build a generator from a corpus of tikified sentences
    # @param sentences [Array<Array<Integer>>]
    # @return [SuffixGenerator]
    def self.build(name, sentences)
      SuffixGenerator.new(name, nil).build(sentences)
    end
    
    def already(sentence)
      min = 10000000000000
      minbigrs = nil
      minpos = nil
      for bi in 2..(sentence.length)
        bigrs = @bigrams[sentence[bi-2]]
        return false if bigrs.nil?
        bigrs = bigrs[sentence[bi-1]]
        return false if bigrs.nil?
        if bigrs.length < min
          min = bigrs.length
          minbigrs = bigrs
          minpos = bi
        end
      end
      minbigrs.each do |ref|
        if ref[1] = minpos # 2 is the first bigram
          if @sentences[ref[0]] == sentence
            return true
          end
        end
      end
      return false
    end
    
    def add(sentence)
      return nil if sentence.length < 2
      tikis=sentence
      begin
        @env.transaction do |trans|
          unless already(tikis) then
            i = @sentences.size
            log ("Adding: sentence #{i}") if (i % 1000) == 0
            @sentences[i] = tikis
            last_tiki = INTERIM
            tikis.each_with_index do |tiki, j|
              raise "wat!" unless last_tiki.is_a?(Integer)
              raise "wat!" unless tiki.is_a?(Integer)
              @unigrams[last_tiki] ||= []
              @unigrams[last_tiki] << [i, j]

              @bigrams[last_tiki] ||= []
              @bigrams[last_tiki][tiki] ||= []

              if j == tikis.length-1 # Mark sentence endings with -1
                @unigrams[tiki] ||= []
                @unigrams[tiki] << [i, -1]
                @bigrams[last_tiki][tiki] << [i, -1]
              else
                @bigrams[last_tiki][tiki] << [i, j+1]
              end

              last_tiki = tiki
            end
          else
            return false
          end
        end
      rescue LMDB::Error::MAP_FULL
        @sentences.expand()
        retry
      end
      return true
    end

    def build(sentences)
      i = @sentences.size
      while (i < sentences.size) 
        log ("Building: sentence #{i}") if (i % 1000) == 0
        tikis = sentences[i]
        add(tikis)
        i += 1
      end
    end

    def initialize(name, prex)
      if prex.nil? then
        dbdir = File.join("model", name)
        puts "Using DB: #{dbdir}"
        FileUtils.mkdir_p(dbdir) unless File.directory?(dbdir)
        @env = LMDB.new dbdir, :nometasync => true, :mapasync => true, :nosync => true
        @sentences = LMDBBackedArray.new(@env.database("sentences", {:create => true}))
        @unigrams = LMDBBackedArray.new(@env.database("unigrams", {:create => true}))
        @bigrams = LMDBBackedArray.new(@env.database("bigrams", {:create => true}))
      else
        @sentences = prex[0]
        @unigrams = prex[1]
        @bigrams = prex[2]
        @env = @sentences.env
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

    def verbatim(sentence)
      min = 10000000000000
      minbigrs = nil
      for bi in 2..(sentence.length)
        bigrs = @bigrams[sentence[bi-2]]
        return false if bigrs.nil?
        bigrs = bigrs[sentence[bi-1]]
        return false if bigrs.nil?
        if bigrs.length < min
          min = bigrs.length
          minbigrs = bigrs
        end
      end
      minbigrs.each do |ref|
        if SuffixGenerator.subseq?(@sentences[ref[0]], sentence)
          return true
        end
      end
      return false
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
          alternatives = alternatives.reject { |a| a[1] == -1 || used.include?(a[0]) }
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
