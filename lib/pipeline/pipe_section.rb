module Pipeline
  class PipeSection
  	attr_reader :source, :target

  	def initialize(
  		source: nil, target: nil,
  		max_workers: 1,
  		&block
  	)
  		raise ArgumentError, "Provide a worker block" unless block_given?
  		@source, @target, @block, @max_workers = source, target, block, max_workers

  		@started = false
  		@workers = []
  	end

  	def start
  		return if @started
  		@started = true

  		@max_workers.times do
  			w = Thread.new(@source, @target, @block) do |source, target, block|
  				until source.empty? && source.closed?
  					payload = source.shift
  					if payload
              result = block.call(payload, source, target)
  						target.push result if target
  					end
  				end
  			end

  			@workers.push w
  		end
  		self
  	end

  	def await
  		start
  		@workers.each { |w| w.join }
  		@target.close if @target
  		self
  	end
  end
end
