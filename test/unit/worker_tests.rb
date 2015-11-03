require 'assert'
require 'dat-worker-pool/worker'

require 'dat-worker-pool'
require 'dat-worker-pool/default_queue'

class DatWorkerPool::Worker

  class UnitTests < Assert::Context
    desc "DatWorkerPool::Worker"
    setup do
      @queue = DatWorkerPool::DefaultQueue.new.tap(&:start)
      @work_done = []
      @worker = DatWorkerPool::Worker.new(@queue).tap do |w|
        w.on_work = proc{ |worker, work| @work_done << work }
      end
    end
    teardown do
      @worker.shutdown
      @queue.shutdown
      @worker.join
    end
    subject{ @worker }

    should have_accessors :on_work, :on_error_callbacks
    should have_accessors :on_start_callbacks, :on_shutdown_callbacks
    should have_accessors :on_sleep_callbacks, :on_wakeup_callbacks
    should have_accessors :before_work_callbacks, :after_work_callbacks
    should have_imeths :start, :shutdown, :join, :raise, :running?

    should "default its callbacks" do
      worker = DatWorkerPool::Worker.new(@queue)
      assert_equal [], worker.on_error_callbacks
      assert_equal [], worker.on_start_callbacks
      assert_equal [], worker.on_shutdown_callbacks
      assert_equal [], worker.on_sleep_callbacks
      assert_equal [], worker.on_wakeup_callbacks
      assert_equal [], worker.before_work_callbacks
      assert_equal [], worker.after_work_callbacks
    end

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

    should "raise an error on the thread using `raise`" do
      subject.on_work = proc do |worker, work|
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

  class CallbacksTests < UnitTests
    desc "callbacks"
    setup do
      @call_counter = 0
      @on_error_called_with    = nil
      @on_start_called_with    = nil
      @on_start_called_at      = nil
      @on_shutdown_called_with = nil
      @on_shutdown_called_at   = nil
      @on_sleep_called_with    = nil
      @on_sleep_called_at      = nil
      @on_wakeup_called_with   = nil
      @on_wakeup_called_at     = nil
      @before_work_called_with = nil
      @before_work_called_at   = nil
      @after_work_called_with  = nil
      @after_work_called_at    = nil
      @worker = DatWorkerPool::Worker.new(@queue).tap do |w|
        w.on_error_callbacks << proc do |*args|
          @on_error_called_with = args
        end
        w.on_start_callbacks << proc do |*args|
          @on_start_called_with = args
          @on_start_called_at   = (@call_counter += 1)
        end
        w.on_shutdown_callbacks << proc do |*args|
          @on_shutdown_called_with = args
          @on_shutdown_called_at   = (@call_counter += 1)
        end
        w.on_sleep_callbacks << proc do |*args|
          @on_sleep_called_with = args
          @on_sleep_called_at   = (@call_counter += 1)
        end
        w.on_wakeup_callbacks << proc do |*args|
          @on_wakeup_called_with = args
          @on_wakeup_called_at   = (@call_counter += 1)
        end
        w.before_work_callbacks << proc do |*args|
          @before_work_called_with = args
          @before_work_called_at   = (@call_counter += 1)
        end
        w.after_work_callbacks << proc do |*args|
          @after_work_called_with = args
          @after_work_called_at   = (@call_counter += 1)
        end
      end
    end

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
