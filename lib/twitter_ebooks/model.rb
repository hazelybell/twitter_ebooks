#!/usr/bin/env ruby
# encoding: utf-8

require 'json'
require 'set'
require 'digest/md5'
require 'csv'

module Ebooks
  class Model
    # @return [Array<String>]
    # An array of unique tokens. This is the main source of actual strings
    # in the model. Manipulation of a token is done using its index
    # in this array, which we call a "tiki"
    attr_accessor :tokens

    # @return [Array<Array<Integer>>]
    # Sentences represented by arrays of tikis
    attr_accessor :sentences

    # @return [Array<Array<Integer>>]
    # Sentences derived from Twitter mentions
    attr_accessor :mentions

    # @return [Array<String>]
    # The top 200 most important keywords, in descending order
    attr_accessor :keywords

    # Generate a new model from a corpus file
    # @param path [String]
    # @return [Ebooks::Model]
    def self.consume(path)
      Model.new.consume(path)
    end

    # Generate a new model from multiple corpus files
    # @param paths [Array<String>]
    # @return [Ebooks::Model]
    def self.consume_all(paths)
      Model.new.consume_all(paths)
    end

    # Load a saved model
    # @param path [String]
    # @return [Ebooks::Model]
    def self.load(path, create=false)
      model = Model.new
      model.instance_eval do
        FileUtils.mkdir_p(path) unless (!create) || File.directory?(path)
        @env = LMDB.new path, :nometasync => true, :mapasync => true, :nosync => true
        puts "Loading the corpus at #{path}"
        @tokens = LMDBBackedArray.new(@env.database("tokens", {:create => create}))
        @sentences = LMDBBackedArray.new(@env.database("sentences", {:create => create}))
        @sentences = LMDBBackedArray.new(@env.database("sentences", {:create => create}))
        @unigrams = LMDBBackedArray.new(@env.database("unigrams", {:create => create}))
        @bigrams = LMDBBackedArray.new(@env.database("bigrams", {:create => create}))
        @smodel = SuffixGenerator.new(nil, [@sentences, @unigrams, @bigrams])
        @mentions = LMDBBackedArray.new(@env.database("mentions", {:create => create}))
        @mention_unigrams = LMDBBackedArray.new(@env.database("mention_unigrams", {:create => create}))
        @mention_bigrams = LMDBBackedArray.new(@env.database("mention_bigrams", {:create => create}))
        @mmodel = SuffixGenerator.new(nil, [@mentions, @mention_unigrams, @mention_bigrams])
        @keywords = LMDBBackedArray.new(@env.database("keywords", {:create => create}))
        if @tokens[INTERIM].nil?
          @tokens[INTERIM] = INTERIMT
        end
        raise "Not INTERIM: #{@tokens[INTERIM]}" unless @tokens[INTERIM] == INTERIMT
        # TODO: persist this properly and not in RAM
        @tokens.each_with_index do |value, index|
          @tikis[value] = index
        end
      end
      model
    end

    # Save model to a file
    # @param path [String]
    def save()
      @env.sync(true)
      self
    end

    # Append a generated model to existing model file instead of overwriting it
    # @param path [String]
    def append(path)
      existing = File.file?(path)
      if !existing
        log "No existing model found at #{path}"
        return
      else
        #read-in and deserialize existing model
        props = Marshal.load(File.open(path,'rb') { |old| old.read })
        old_tokens = props[:tokens]
        old_sentences = props[:sentences]
        old_mentions = props[:mentions]
        old_keywords = props[:keywords]

        #append existing properties to new ones and overwrite with new model
        File.open(path, 'wb') do |f|
          f.write(Marshal.dump({
            tokens: @tokens.concat(old_tokens),
            sentences: @sentences.concat(old_sentences),
            mentions: @mentions.concat(old_mentions),
            keywords: @keywords.concat(old_keywords)
          }))
        end
      end
      self
    end
     

    def initialize
#       @tokens = []

      # Reverse lookup tiki by token, for faster generation
      @tikis = {}
    end

    # Reverse lookup a token index from a token
    # @param token [String]
    # @return [Integer]
    def tikify(token)
      if @tikis.has_key?(token) then
        return @tikis[token]
      else
        (@tokens.length+1)%1000 == 0 and puts "#{@tokens.length+1} tokens"
        begin
          @tokens << token 
        rescue LMDB::Error::MAP_FULL
          @tokens.expand()
          retry
        end
        @tikis[token] = @tokens.length-1
      end
    end

    # Convert a body of text into arrays of tikis
    # @param text [String]
    # @param destination [Array<Array<Integer>>]
    # @return [Array<Array<Integer>>]
    def mass_tikify(text, destination)
      sentences = text.lines
      i = 0
      sentences.each do |s|
        log ("Importing: sentence #{i}") if (i % 1000) == 0
        begin
#           @env.transaction do |trans|
            tokens = NLP.tokenize(s).reject do |t|
              # Don't include usernames/urls as tokens
              (t[0]==('@')) \
                  || (t.include?('://')) \
                  || t.length > 30
            end
            tokstr = tokens.map { |t| tikify(t) }
            destination.add(tokstr)
#           end
        rescue LMDB::Error::MAP_FULL
          destination.expand()
          retry
        end
        i += 1
      end
      destination
    end

    # Consume a corpus into this model
    # @param path [String]
    def consume(path)
      content = File.read(path, :encoding => 'utf-8')

      if path.split('.')[-1] == "json"
        log "Reading json corpus from #{path}"
        lines = JSON.parse(content).map do |tweet|
          tweet['text']
        end
      elsif path.split('.')[-1] == "csv"
        log "Reading CSV corpus from #{path}"
        content = CSV.parse(content)
        header = content.shift
        text_col = header.index('text')
        lines = content.map do |tweet|
          tweet[text_col]
        end
      else
        log "Reading plaintext corpus from #{path} (if this is a json or csv file, please rename the file with an extension and reconsume)"
        lines = content.split("\n")
      end

      consume_lines(lines)
    end

    # Consume a sequence of lines
    # @param lines [Array<String>]
    def consume_lines(lines)
      log "Removing commented lines and sorting mentions"

      statements = []
      mentions = []
      lines.each do |l|
        next if l.start_with?('#') # Remove commented lines
        next if l.include?('RT') || l.include?('MT') # Remove soft retweets

        if l[0]==('@') || l.match(/\s@/)
          mentions << NLP.normalize(l)
        else
          statements << NLP.normalize(l)
        end
      end

      text = statements.join("\n").encode('UTF-8', :invalid => :replace)
      mention_text = mentions.join("\n").encode('UTF-8', :invalid => :replace)

      lines = nil; statements = nil; mentions = nil # Allow garbage collection

      log "Tokenizing #{text.count('\n')} statements and #{mention_text.count('\n')} mentions"

      mass_tikify(text, @smodel)
      mass_tikify(mention_text, @mmodel)

      @keywords.import(NLP.keywords(text).top(2000).map(&:to_s))
      log "Top keywords: #{@keywords[0]} #{@keywords[1]} #{@keywords[2]}"

      self
    end

    # Consume multiple corpuses into this model
    # @param paths [Array<String>]
    def consume_all(paths)
      lines = []
      paths.each do |path|
        content = File.read(path, :encoding => 'utf-8')

        if path.split('.')[-1] == "json"
          log "Reading json corpus from #{path}"
          l = JSON.parse(content).map do |tweet|
            tweet['text']
          end
          lines.concat(l)
        elsif path.split('.')[-1] == "csv"
          log "Reading CSV corpus from #{path}"
          content = CSV.parse(content)
          header = content.shift
          text_col = header.index('text')
          l = content.map do |tweet|
            tweet[text_col]
          end
          lines.concat(l)
        else
          log "Reading plaintext corpus from #{path}"
          l = content.split("\n")
          lines.concat(l)
        end
      end
      consume_lines(lines)
    end

    # Correct encoding issues in generated text
    # @param text [String]
    # @return [String]
    def fix(text)
      NLP.htmlentities.decode text
    end

    # Check if an array of tikis comprises a valid tweet
    # @param tikis [Array<Integer>]
    # @param limit Integer how many chars we have left
    def valid_tweet?(tikis, limit)
      return false if tikis.nil?
      tweet = NLP.reconstruct(tikis, @tokens)
      tweet.length <= limit && !NLP.unmatched_enclosers?(tweet)
    end

    # Generate some text
    # @param limit [Integer] available characters
    # @param generator [SuffixGenerator, nil]
    # @param retry_limit [Integer] how many times to retry on invalid tweet
    # @return [String]
    def make_statement(name, limit=140, generator=nil, retry_limit=100, min_length=3)
      generator = @smodel if generator.nil?

      retries = 0
      tweet = ""
      verbatim = false

      while (retries <= retry_limit/2) do
        tikis = generator.generate(3, :bigrams)
        log "Attempting to produce tweet try #{retries+1}/#{retry_limit}"
        next if tikis.length <= min_length
        verbatim = verbatim?(tikis)
        break if (!verbatim) && valid_tweet?(tikis, limit)
        puts "Verbatim: #{NLP.reconstruct(tikis, @tokens)}" if verbatim
        retries += 1
      end

      if verbatim  # We made a verbatim tweet by accident
        while (retries <= retry_limit) do
          log "Attempting to produce unigram tweet try #{retries+1}/#{retry_limit}"
          tikis = generator.generate(3, :unigrams)
          break if valid_tweet?(tikis, limit) && !verbatim?(tikis)

          retries += 1
        end
      end

      tweet = NLP.reconstruct(tikis, @tokens)

      if retries >= retry_limit
        log "Unable to produce valid non-verbatim tweet..."
        return nil
      end

      fix tweet
    end

    # Test if a sentence has been copied verbatim from original
    # @param tikis [Array<Integer>]
    # @return [Boolean]
    def verbatim?(tikis)
      return (@smodel.verbatim(tikis) || @mmodel.verbatim(tikis))
    end

    # Finds relevant and slightly relevant tokenized sentences to input
    # comparing non-stopword token overlaps
    # @param sentences [Array<Array<Integer>>]
    # @param input [String]
    # @return [Array<Array<Array<Integer>>, Array<Array<Integer>>>]
    def find_relevant(sentences, input)
      relevant = []
      slightly_relevant = []

      tokenized = NLP.tokenize(input).map(&:downcase)

      sentences.each do |sent|
        tokenized.each do |token|
          if sent.map { |tiki| @tokens[tiki].downcase }.include?(token)
            relevant << sent unless NLP.stopword?(token)
            slightly_relevant << sent
          end
        end
      end

      [relevant, slightly_relevant, tokenized]
    end

    # Generates a response by looking for related sentences
    # in the corpus and building a smaller generator from these
    # @param input [String]
    # @param limit [Integer] characters available for response
    # @param sentences [Array<Array<Integer>>]
    # @return [String]
    def make_response(name, input, limit=140, sentences=(@mentions))
      # Prefer mentions
      # Turned this off for now its causing fail?
      relevant, slightly_relevant, tokenized = find_relevant(sentences, input)
      tokenized = tokenized.map {|tok| tok.gsub(/\W/, '_')}
      tokstr = tokenized.join(".")
      puts "Making response for #{tokstr} "
      if relevant.length >= 30
#         if (relevant.length >= 100000)
#           relevant = relevant.sample(100001)
#         end
        name = name + "/" + tokstr
        generator = SuffixGenerator.build(name, relevant)
        make_statement(name, limit, generator)
      elsif slightly_relevant.length >= 50
#         if (slightly_relevant.length >= 100000)
#           slightly_relevant = slightly_relevant.sample(100002)
#         end
        name = name + "/" + tokstr
        generator = SuffixGenerator.build(name, slightly_relevant)
        make_statement(name, limit, generator)
      elsif sentences.equal?(@mentions)
        make_response(name, input, limit, @sentences)
      else
        make_statement(name, limit)
      end
    end
  end
end
