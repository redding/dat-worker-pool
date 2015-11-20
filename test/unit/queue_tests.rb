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

    should "raise a not implemented error for `dwp_push` and `dwp_pop` by default" do
      queue_class = Class.new{ include DatWorkerPool::Queue }
      queue = queue_class.new.tap(&:dwp_start)

      assert_raises(NotImplementedError){ queue.dwp_push(Factory.string) }
      assert_raises(NotImplementedError){ queue.dwp_pop }
    end

  end

  class InitTests < UnitTests
    desc "when init"
    setup do
      @queue = @queue_class.new
    end
    subject{ @queue }

    should have_imeths :work_items
    should have_imeths :dwp_start, :dwp_signal_shutdown, :dwp_shutdown
    should have_imeths :running?, :shutdown?
    should have_imeths :dwp_push, :dwp_pop

    should "raise a not implemented error using `work_items`" do
      assert_raises(NotImplementedError){ subject.work_items }
    end

    should "set its flags using `dwp_start` and `dwp_shutdown`" do
      assert_false subject.running?
      assert_true  subject.shutdown?
      subject.dwp_start
      assert_true  subject.running?
      assert_false subject.shutdown?
      subject.dwp_shutdown
      assert_false subject.running?
      assert_true  subject.shutdown?
    end

    should "call `start!` using `dwp_start`" do
      assert_false subject.start_called
      subject.dwp_start
      assert_true subject.start_called
    end

    should "set its shutdown flag using `dwp_signal_shutdown`" do
      assert_false subject.running?
      assert_false subject.shutdown_called
      subject.dwp_start
      assert_true  subject.running?
      assert_false subject.shutdown_called
      subject.dwp_signal_shutdown
      assert_false subject.running?
      assert_false subject.shutdown_called
    end

    should "call `shutdown!` using `dwp_shutdown`" do
      assert_false subject.shutdown_called
      subject.dwp_shutdown
      assert_true subject.shutdown_called
    end

    should "raise an error if `dwp_push` is called when the queue isn't running" do
      assert_false subject.running?
      assert_raise(RuntimeError){ subject.dwp_push(Factory.string) }
      subject.dwp_start
      assert_nothing_raised{ subject.dwp_push(Factory.string) }
      subject.dwp_shutdown
      assert_raise(RuntimeError){ subject.dwp_push(Factory.string) }
    end

    should "call `push!` using `dwp_push`" do
      subject.dwp_start

      work_item = Factory.string
      subject.dwp_push(work_item)
      assert_equal [work_item], subject.push_called_with

      args = Factory.integer(3).times.map{ Factory.string }
      subject.dwp_push(*args)
      assert_equal args, subject.push_called_with
    end

    should "return nothing if `dwp_pop` is called when the queue isn't running" do
      subject.pop_result = Factory.string
      assert_false subject.running?
      assert_nil subject.dwp_pop
      subject.dwp_start
      assert_not_nil subject.dwp_pop
      subject.dwp_shutdown
      assert_nil subject.dwp_pop
    end

    should "call `pop!` using `dwp_pop`" do
      subject.dwp_start
      subject.pop_result = Factory.string

      value = subject.dwp_pop
      assert_equal subject.pop_result, value
    end

  end

end
