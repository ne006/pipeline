require 'rspec'

RSpec.describe Pipeline::PipeSection do
	describe "#start" do
		before(:each) do
			@source = Queue.new
			@target = Queue.new

			(1..5).each { |i| @source.push i }

			@section = Pipeline::PipeSection.new(
				source: @source,
				target: @target,
				max_workers: 3
			) do |i, source, target|
				i*2
			end
		end

		it "should get items from the source queue" do
			@source.close
			@section.await

			expect(@source.size).to eql(0)
		end

		it "should put resulting items in the target queue" do
			source_size = @source.size
			@source.close
			@section.await

			expect(@target.size).to eql(source_size)
		end

		it "should process items" do
			@source.close
			@section.await

			result = (1..5).to_a.map { @target.shift }

			expect(result).to match_array (1..5).map { |i| i*2 }
		end
	end
end
