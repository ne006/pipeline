require 'rspec'

RSpec.describe Pipeline::Pipeline do
	describe "#start" do
		before(:each) do
			@source = Queue.new
			@target = Queue.new

			(1..5).each { |i| @source.push i }

			@pipeline = Pipeline::Pipeline.new(
				source: @source,
				target: @target,
				sequence: [
					{	block: (proc { |e| e*2 })	},
					{
						block: (proc { |e| e.pow(2) }),
						max_workers: 3
					}
				]
			)
		end

		it "should get items from the source queue" do
			@source.close
			@pipeline.await

			expect(@source.size).to eql(0)
		end

		it "should put resulting items in the target queue" do
			source_size = @source.size
			@source.close
			@pipeline.await

			expect(@target.size).to eql(source_size)
		end

		it "should process items" do
			@source.close
			@pipeline.await

			result = (1..5).to_a.map { @target.shift }

			expect(result).to match_array (1..5).map { |i| i*2 }.map { |i| i.pow(2) }
		end
	end
end
