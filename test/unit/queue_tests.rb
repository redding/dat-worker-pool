require 'assert'
require 'dat-worker-pool/queue'

module DatWorkerPool::Queue

  class UnitTests < Assert::Context
    desc "DatWorkerPool::Queue"
    setup do
      @queue_class = Class.new do
        include DatWorkerPool::Queue

        attr_reader :start_called, :shutdown_called
        attr_reader :push_called_with
        attr_accessor :pop_result

        def initialize
          @start_called     = false
          @shutdown_called  = false
          @push_called_with = nil
          @pop_result       = nil
        end

        def start!;    @start_called = true;    end
        def shutdown!; @shutdown_called = true; end

        def push!(*args); @push_called_with = args; end
        def pop!; @pop_result; end
      end
    end
    subject{ @queue_class }

    should "raise a not implemented error for `push` and `pop` by default" do
      queue_class = Class.new{ include DatWorkerPool::Queue }
      queue = queue_class.new.tap(&:start)

      assert_raises(NotImplementedError){ queue.push(Factory.string) }
      assert_raises(NotImplementedError){ queue.pop }
    end

  end

  class InitTests < UnitTests
    desc "when init"
    setup do
      @queue = @queue_class.new
    end
    subject{ @queue }

    should have_imeths :start, :shutdown, :running?, :shutdown?
    should have_imeths :push, :pop

    should "set its flags using `start` and `shutdown`" do
      assert_false subject.running?
      assert_true  subject.shutdown?
      subject.start
      assert_true  subject.running?
      assert_false subject.shutdown?
      subject.shutdown
      assert_false subject.running?
      assert_true  subject.shutdown?
    end

    should "call `start!` using `start`" do
      assert_false subject.start_called
      subject.start
      assert_true subject.start_called
    end

    should "call `shutdown!` using `shutdown`" do
      assert_false subject.shutdown_called
      subject.shutdown
      assert_true subject.shutdown_called
    end

    should "raise an error if `push` is called when the queue isn't running" do
      assert_false subject.running?
      assert_raise(RuntimeError){ subject.push(Factory.string) }
      subject.start
      assert_nothing_raised{ subject.push(Factory.string) }
      subject.shutdown
      assert_raise(RuntimeError){ subject.push(Factory.string) }
    end

    should "call `push!` using `push`" do
      subject.start

      work_item = Factory.string
      subject.push(work_item)
      assert_equal [work_item], subject.push_called_with

      args = Factory.integer(3).times.map{ Factory.string }
      subject.push(*args)
      assert_equal args, subject.push_called_with
    end

    should "return nothing if `pop` is called when the queue isn't running" do
      subject.pop_result = Factory.string
      assert_false subject.running?
      assert_nil subject.pop
      subject.start
      assert_not_nil subject.pop
      subject.shutdown
      assert_nil subject.pop
    end

    should "call `pop!` using `pop`" do
      subject.start
      subject.pop_result = Factory.string

      value = subject.pop
      assert_equal subject.pop_result, value
    end

  end

end
