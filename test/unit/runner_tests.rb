require 'assert'
require 'dat-worker-pool/runner'

require 'much-timeout'
require 'dat-worker-pool/default_queue'
require 'test/support/signal_test_worker'

class DatWorkerPool::Runner

  class UnitTests < Assert::Context
    include SignalTestWorker::TestHelpers

    desc "DatWorkerPool::Runner"
    setup do
      @runner_class = DatWorkerPool::Runner
    end
    subject{ @runner_class }

  end

  class InitTests < UnitTests
    desc "when init"
    setup do
      @logger       = TEST_LOGGER || Logger.new("/dev/null")
      @queue        = DatWorkerPool::DefaultQueue.new
      @worker_class = TestWorker

      @worker_params.merge!({
        Factory.string => Factory.string
      })

      @workers = DatWorkerPool::LockedArray.new
      Assert.stub(DatWorkerPool::LockedArray, :new){ @workers }

      @available_workers_spy = DatWorkerPool::LockedSet.new
      Assert.stub(DatWorkerPool::LockedSet, :new){ @available_workers_spy }

      @options = {
        :num_workers   => @num_workers,
        :logger        => @logger,
        :queue         => @queue,
        :worker_class  => @worker_class,
        :worker_params => @worker_params
      }
      @runner = @runner_class.new(@options)
    end
    teardown do
      @runner.shutdown(0) rescue false
    end
    subject{ @runner }

    should have_readers :num_workers, :worker_class, :worker_params
    should have_readers :logger_proxy, :queue
    should have_imeths :workers, :start, :shutdown
    should have_imeths :available_worker_count, :worker_available?
    should have_imeths :make_worker_available, :make_worker_unavailable
    should have_imeths :worker_log

    should "know its attributes" do
      assert_equal @num_workers,   subject.num_workers
      assert_equal @worker_class,  subject.worker_class
      assert_equal @worker_params, subject.worker_params
      assert_equal @queue,         subject.queue

      assert_instance_of LoggerProxy, subject.logger_proxy
      assert_equal @logger, subject.logger_proxy.logger
    end

    should "default its logger" do
      @options.delete(:logger)
      runner = @runner_class.new(@options)
      assert_instance_of NullLoggerProxy, runner.logger_proxy
    end

    should "know its workers" do
      assert_equal @workers.values, subject.workers
      @workers.push(Factory.string)
      assert_equal @workers.values, subject.workers
    end

    should "allow making workers available/unavailable" do
      worker = @worker_class.new(@runner, @queue, Factory.integer(10))

      assert_not_includes worker.object_id, @available_workers_spy.values
      assert_false subject.worker_available?
      subject.make_worker_available(worker)
      assert_includes worker.object_id, @available_workers_spy.values
      assert_true subject.worker_available?
      subject.make_worker_unavailable(worker)
      assert_not_includes worker.object_id, @available_workers_spy.values
      assert_false subject.worker_available?
    end

    should "know how many workers are available" do
      worker = @worker_class.new(@runner, @queue, Factory.integer(10))

      assert_equal 0, subject.available_worker_count
      subject.make_worker_available(worker)
      assert_equal 1, subject.available_worker_count
      subject.make_worker_unavailable(worker)
      assert_equal 0, subject.available_worker_count
    end

    should "allow logging messages using `log`" do
      logged_message = nil
      Assert.stub(subject.logger_proxy, :runner_log) do |&mb|
        logged_message = mb.call
      end

      text = Factory.text
      subject.log{ text }
      assert_equal text, logged_message
    end

    should "allow workers to log messages using `worker_log`" do
      passed_worker  = nil
      logged_message = nil
      Assert.stub(subject.logger_proxy, :worker_log) do |w, &mb|
        passed_worker  = w
        logged_message = mb.call
      end
      worker = @worker_class.new(@runner, @queue, Factory.integer(10))

      text = Factory.text
      subject.worker_log(worker){ text }
      assert_same worker, passed_worker
      assert_equal text, logged_message
    end

  end

  class StartTests < InitTests
    desc "and started"

    should "start its queue" do
      assert_false @queue.running?
      subject.start
      assert_true @queue.running?
    end

    should "build and add workers" do
      subject.start
      wait_for_workers_to_become_available

      assert_equal @num_workers, subject.workers.size
      subject.workers.each_with_index do |worker, n|
        assert_equal subject, worker.dwp_runner
        assert_equal @queue,  worker.dwp_queue
        assert_equal n + 1,   worker.dwp_number
        assert_true worker.dwp_running?
      end
    end

  end

  class ShutdownTests < StartTests
    desc "and shutdown"
    setup do
      @timeout_seconds         = nil
      @optional_timeout_called = false
      # this acts as a spy but also keeps the shutdown from ever timing out
      Assert.stub(MuchTimeout, :just_optional_timeout) do |secs, args|
        @timeout_seconds         = secs
        @optional_timeout_called = true
        args[:do].call
      end

      @runner.start
      wait_for_workers_to_become_available

      # we need a reference to the workers, the runners workers will get removed
      # as they shutdown
      @running_workers = @runner.workers.dup
    end

    should "optionally timeout when shutdown" do
      subject.shutdown
      assert_nil @timeout_seconds
      assert_true @optional_timeout_called

      @optional_timeout_called = false
      seconds = Factory.integer
      subject.shutdown(seconds)
      assert_equal seconds, @timeout_seconds
      assert_true @optional_timeout_called
    end

    should "shutdown all of its workers" do
      @running_workers.each do |worker|
        assert_false worker.dwp_shutdown?
      end

      subject.shutdown(Factory.boolean ? Factory.integer : nil)
      wait_for_workers_to_become_unavailable

      @running_workers.each do |worker|
        assert_true worker.dwp_shutdown?
      end
    end

    should "shutdown its queue" do
      assert_false @queue.shutdown?
      subject.shutdown(Factory.boolean ? Factory.integer : nil)
      assert_true @queue.shutdown?
    end

    should "join its workers waiting for them to finish" do
      @running_workers.each do |worker|
        assert_false worker.join_called
      end

      subject.shutdown(Factory.boolean ? Factory.integer : nil)
      wait_for_workers_to_become_unavailable

      @running_workers.each do |worker|
        assert_true worker.join_called
      end
    end

    should "join all workers even if one raises an error when joined" do
      @running_workers.sample.join_error = Factory.exception

      subject.shutdown(Factory.boolean ? Factory.integer : nil)
      wait_for_workers_to_become_unavailable

      @running_workers.each do |worker|
        assert_true worker.join_called
      end
    end

    should "remove workers as they finish" do
      assert_false subject.workers.empty?

      subject.shutdown(Factory.boolean ? Factory.integer : nil)
      wait_for_workers_to_become_unavailable

      assert_true subject.workers.empty?
    end

    should "remove workers and make them unavailable even if they error" do
      @running_workers.each{ |w| w.join_error = Factory.exception }

      assert_false subject.workers.empty?
      assert_false @available_workers_spy.empty?

      subject.shutdown(Factory.boolean ? Factory.integer : nil)
      wait_for_workers_to_become_unavailable

      assert_true subject.workers.empty?
      assert_true @available_workers_spy.empty?
    end

    should "force its workers to shutdown if a timeout error occurs" do
      Assert.stub(MuchTimeout, :just_optional_timeout) do |secs, args|
        args[:on_timeout].call # force an immediate timeout
      end

      @num_workers.times{ @queue.dwp_push(:hang) }
      wait_for_workers_to_become_unavailable
      subject.shutdown(Factory.integer)

      @running_workers.each do |worker|
        assert_instance_of DatWorkerPool::ShutdownError, worker.raised_error
        assert_true worker.join_called
      end
      assert_true subject.workers.empty?
      assert_true @available_workers_spy.empty?
    end

    should "force its workers to shutdown if a non-timeout error occurs" do
      queue_exception = Factory.exception
      Assert.stub(@queue, :dwp_shutdown){ raise queue_exception }

      @num_workers.times{ @queue.dwp_push(:hang) }
      wait_for_workers_to_become_unavailable

      caught_exception = nil
      begin
        subject.shutdown(Factory.integer)
      rescue StandardError => caught_exception
      end
      assert_same queue_exception, caught_exception

      @running_workers.each do |worker|
        assert_instance_of DatWorkerPool::ShutdownError, worker.raised_error
        assert_true worker.join_called
      end
      assert_true subject.workers.empty?
      assert_true @available_workers_spy.empty?
    end

    should "force shutdown all of its workers even if one raises an error when joining" do
      Assert.stub(MuchTimeout, :just_optional_timeout) do |secs, args|
        args[:on_timeout].call # force an immediate timeout
      end
      error_class  = Factory.boolean ? DatWorkerPool::ShutdownError : RuntimeError
      error_worker = @running_workers.sample
      error_worker.join_error = Factory.exception(error_class)

      @num_workers.times{ @queue.dwp_push(:hang) }
      wait_for_workers_to_become_unavailable
      subject.shutdown(Factory.boolean ? Factory.integer : nil)

      (@running_workers - [error_worker]).each do |worker|
        assert_instance_of DatWorkerPool::ShutdownError, worker.raised_error
        assert_true worker.join_called
      end
      assert_instance_of error_class, error_worker.raised_error
      assert_true error_worker.join_called

      assert_true subject.workers.empty?
      assert_true @available_workers_spy.empty?
    end

  end

  class LoggerProxyTests < UnitTests
    desc "LoggerProxy"
    setup do
      @stringio     = StringIO.new
      @logger       = Logger.new(@stringio)
      @logger_proxy = LoggerProxy.new(@logger)
    end
    subject{ @logger_proxy }

    should have_readers :logger
    should have_imeths :runner_log, :worker_log

    should "know its logger" do
      assert_equal @logger, subject.logger
    end

    should "log a message block for a runner using `runner_log`" do
      text = Factory.text
      subject.runner_log{ text }
      assert_match "[DWP] #{text}", @stringio.string
    end

    should "log a message block for a worker using `worker_log`" do
      worker = FakeWorker.new(Factory.integer(10))
      text   = Factory.text
      subject.worker_log(worker){ text }
      assert_match "[DWP-#{worker.dwp_number}] #{text}", @stringio.string
    end

  end

  class NullLoggerProxyTests < UnitTests
    desc "NullLoggerProxy"
    setup do
      @null_logger_proxy = NullLoggerProxy.new
    end
    subject{ @null_logger_proxy }

    should have_imeths :runner_log, :worker_log

  end

  class TestWorker
    include SignalTestWorker

    attr_reader :join_called
    attr_accessor :join_error, :raised_error

    # for testing what is passed to the worker
    attr_reader :dwp_runner, :dwp_queue

    on_available{   signal_test_suite_thread }
    on_unavailable{ signal_test_suite_thread }

    on_shutdown{ raise self.join_error if self.join_error }

    on_error{ |e, wi| self.raised_error = e }

    def initialize(*args)
      super
      @join_called = false
      @join_error  = nil

      @hang_mutex    = Mutex.new
      @hang_cond_var = ConditionVariable.new
    end

    def work!(work_item)
      case(work_item)
      when :hang
        MuchTimeout.timeout(1) do
          @hang_mutex.synchronize{ @hang_cond_var.wait(@hang_mutex) }
        end
      end
    end

    def dwp_join(*args)
      @join_called = true
      super
    end

  end

  FakeWorker = Struct.new(:dwp_number)

end
