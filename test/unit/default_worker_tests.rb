require 'assert'
require 'dat-worker-pool/default_worker'

require 'dat-worker-pool'
require 'dat-worker-pool/default_queue'
require 'dat-worker-pool/runner'
require 'dat-worker-pool/worker'

class DatWorkerPool::DefaultWorker

  class UnitTests < Assert::Context
    desc "DatWorkerPool::DefaultWorker"
    setup do
      @worker_class = DatWorkerPool::DefaultWorker
    end
    subject{ @worker_class }

    should "be a dat worker pool worker" do
      assert_includes DatWorkerPool::Worker, subject
    end

  end

  class InitSetupTests < UnitTests
    desc "when init"
    setup do
      @queue  = DatWorkerPool::DefaultQueue.new.tap(&:start)
      @runner = DatWorkerPool::Runner.new(:queue => @queue)
    end
    teardown do
      @worker.shutdown if @worker
      @queue.shutdown
      @worker.join if @worker
    end
    subject{ @worker }

  end

  class InitTests < InitSetupTests
    setup do
      @work_done = []
      @worker = @worker_class.new(@runner, @queue).tap do |w|
        w.on_work = proc{ |work| @work_done << work }
      end
    end

    should have_accessors :on_work
    should have_imeths :start, :shutdown, :join, :raise, :running?

    should "start a thread with it's work loop using `start`" do
      thread = nil
      assert_nothing_raised{ thread = subject.start }
      assert_instance_of Thread, thread
      assert thread.alive?
      assert subject.running?
    end

    should "call the block it's passed when it get's work from the queue" do
      subject.start
      @queue.push 'one'
      subject.join 0.1 # trigger the worker's thread to run
      @queue.push 'two'
      subject.join 0.1 # trigger the worker's thread to run
      assert_equal [ 'one', 'two' ], @work_done
    end

    should "increment and decrement its runners workers waiting count" do
      increment_call_count = 0
      Assert.stub(@runner, :increment_worker_waiting){ increment_call_count += 1 }
      decrement_call_count = 0
      Assert.stub(@runner, :decrement_worker_waiting){ decrement_call_count += 1 }

      subject.start
      @queue.push Factory.string

      assert_equal 2, increment_call_count
      assert_equal 1, decrement_call_count
    end

    should "flag itself for exiting it's work loop using `shutdown` and " \
           "end it's thread once it's queue is shutdown" do
      thread = subject.start
      subject.join 0.1 # trigger the worker's thread to run, allow it to get into it's
                       # work loop
      assert_nothing_raised{ subject.shutdown }
      @queue.shutdown

      subject.join 0.1 # trigger the worker's thread to run, should exit
      assert_not thread.alive?
      assert_not subject.running?
    end

    should "despawn itself from its runner when shutdown" do
      despawned_worker = nil
      Assert.stub(@runner, :despawn_worker){ |worker| despawned_worker = worker }

      subject.start
      subject.join 0.1 # trigger the worker's thread to run
      assert_nothing_raised{ subject.shutdown }
      @queue.shutdown

      assert_same subject, despawned_worker
    end

    should "raise an error on the thread using `raise`" do
      subject.on_work = proc do |work|
        begin
          sleep 1
        rescue RuntimeError => error
          @work_done << error
          raise error
        end
      end
      subject.start
      @queue.push 'a'
      subject.join 0.1 # trigger the worker's thread to run

      exception = RuntimeError.new
      subject.raise exception
      assert_equal [exception], @work_done
    end

  end

  class CallbacksTests < InitSetupTests
    setup do
      @call_counter = 0

      @on_start_called_with = nil
      @on_start_called_at   = nil
      @worker_class.on_start do |*args|
        @on_start_called_with = args
        @on_start_called_at   = (@call_counter += 1)
      end

      @on_shutdown_called_with = nil
      @on_shutdown_called_at   = nil
      @worker_class.on_shutdown do |*args|
        @on_shutdown_called_with = args
        @on_shutdown_called_at   = (@call_counter += 1)
      end

      @on_sleep_called_with = nil
      @on_sleep_called_at   = nil
      @worker_class.on_sleep do |*args|
        @on_sleep_called_with = args
        @on_sleep_called_at   = (@call_counter += 1)
      end

      @on_wakeup_called_with = nil
      @on_wakeup_called_at   = nil
      @worker_class.on_wakeup do |*args|
        @on_wakeup_called_with = args
        @on_wakeup_called_at   = (@call_counter += 1)
      end

      @on_error_called_with = nil
      @worker_class.on_error{ |*args| @on_error_called_with = args }

      @before_work_called_with = nil
      @before_work_called_at   = nil
      @worker_class.before_work do |*args|
        @before_work_called_with = args
        @before_work_called_at   = (@call_counter += 1)
      end

      @after_work_called_with  = nil
      @after_work_called_at    = nil
      @worker_class.after_work do |*args|
        @after_work_called_with = args
        @after_work_called_at   = (@call_counter += 1)
      end

      @worker = @worker_class.new(@runner, @queue)
    end
    subject{ @worker }

    should "pass its self to its start, shutdown, sleep and wakeup callbacks" do
      subject.start
      @queue.push('work')
      subject.shutdown
      @queue.shutdown

      assert_equal [subject], @on_start_called_with
      assert_equal [subject], @on_shutdown_called_with
      assert_equal [subject], @on_sleep_called_with
      assert_equal [subject], @on_wakeup_called_with
    end

    should "pass its self and work to its before and after work callbacks" do
      subject.start
      @queue.push('work')
      subject.shutdown
      @queue.shutdown

      assert_equal [subject, 'work'], @before_work_called_with
      assert_equal [subject, 'work'], @after_work_called_with
    end

    should "call its callbacks throughout its lifecycle" do
      subject.start
      assert_equal 1, @on_start_called_at
      assert_equal 2, @on_sleep_called_at
      @queue.push('work')
      assert_equal 3, @on_wakeup_called_at
      assert_equal 4, @before_work_called_at
      assert_equal 5, @after_work_called_at
      assert_equal 6, @on_sleep_called_at
      subject.shutdown
      @queue.shutdown
      assert_equal 7, @on_wakeup_called_at
      assert_equal 8, @on_shutdown_called_at
    end

    should "call its error callbacks when an exception occurs" do
      exception = RuntimeError.new
      subject.on_work = proc{ raise exception }
      thread = subject.start
      @queue.push('work')
      assert_equal [subject, exception, 'work'], @on_error_called_with
      assert_true thread.alive?
    end

    should "call its error callbacks when an shutdown error occurs and reraise" do
      exception = DatWorkerPool::ShutdownError.new
      subject.on_work = proc{ raise exception }
      thread = subject.start
      @queue.push('work')
      assert_equal [subject, exception, 'work'], @on_error_called_with
      assert_false thread.alive?
      # ensure the shutdown error is handled and isn't thrown when we join
      assert_nothing_raised{ thread.join }
    end

  end

end
