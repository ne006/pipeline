require 'pipeline/pipe_section'
require 'pipeline/version'

module Pipeline
  class Pipeline
  	attr_accessor :source, :target

  	def initialize(
  		source: nil,
  		target: nil,
  		sequence: []
  	)
  		@source, @target = source, target

  		@started = false
  		@sequence = []

  		sequence.each_with_index do |step, num|
  			max_workers = 1
  			block = nil

  			if step.is_a? Hash
  				raise ArgumentError, "step #{num} block is not specified" unless step[:block]

  				block = step[:block]
  				max_workers = step[:max_workers] if step[:max_workers].is_a? Integer
  			elsif step.respond_to? :call
  				block = step
  			end

  			@sequence.push PipeSection.new(
  				source: (num.zero? ? @source : @sequence[num - 1].target),
  				target: (num == sequence.size - 1 ? @target : Queue.new),
  				max_workers: max_workers,
  				&block
  			)
  		end
  	end

  	def start
  		return if @started
  		@started = true

  		@sequence.each(&:start)
  		self
  	end

  	def await
  		start
  		@sequence.each(&:await)
  		self
  	end
  end
end
