require 'logger'

require 'dat-worker-pool/version'
require 'dat-worker-pool/default_worker'
require 'dat-worker-pool/queue'
require 'dat-worker-pool/runner'

class DatWorkerPool

  DEFAULT_NUM_WORKERS = 1
  MIN_WORKERS         = 1

  attr_reader :logger, :queue

  def initialize(options = nil)
    options ||= {}
    num_workers = (options[:num_workers] || DEFAULT_NUM_WORKERS).to_i
    if num_workers < MIN_WORKERS
      raise ArgumentError, "number of workers must be at least #{MIN_WORKERS}"
    end

    @logger = options[:logger] || NullLogger.new

    @queue = options[:queue] || begin
      require 'dat-worker-pool/default_queue'
      DatWorkerPool::DefaultQueue.new
    end

    # TODO - don't save off once a worker class can be passed in
    @worker_class = DefaultWorker
    @runner = DatWorkerPool::Runner.new({
      :num_workers  => num_workers,
      :queue        => @queue,
      :worker_class => @worker_class,
      :do_work_proc => options[:do_work_proc]
    })
  end

  def start
    @runner.start
  end

  def shutdown(timeout = nil)
    @runner.shutdown(timeout)
  end

  def add_work(work_item)
    return if work_item.nil?
    @queue.push work_item
  end

  def num_workers
    @runner.num_workers
  end

  def waiting
    @runner.workers_waiting_count
  end

  def worker_available?
    self.waiting > 0
  end

  # TODO - remove once a worker class can be passed in
  def on_worker_start_callbacks;    @worker_class.on_start_callbacks;    end
  def on_worker_shutdown_callbacks; @worker_class.on_shutdown_callbacks; end
  def on_worker_sleep_callbacks;    @worker_class.on_sleep_callbacks;    end
  def on_worker_wakeup_callbacks;   @worker_class.on_wakeup_callbacks;   end
  def on_worker_error_callbacks;    @worker_class.on_error_callbacks;    end
  def before_work_callbacks;        @worker_class.before_work_callbacks; end
  def after_work_callbacks;         @worker_class.after_work_callbacks;  end

  # TODO - remove once a worker class can be passed in
  def on_worker_start(&block);    @worker_class.on_start(&block);    end
  def on_worker_shutdown(&block); @worker_class.on_shutdown(&block); end
  def on_worker_sleep(&block);    @worker_class.on_sleep(&block);    end
  def on_worker_wakeup(&block);   @worker_class.on_wakeup(&block);   end
  def on_worker_error(&block);    @worker_class.on_error(&block);    end
  def before_work(&block);        @worker_class.before_work(&block); end
  def after_work(&block);         @worker_class.after_work(&block);  end

  class NullLogger
    ::Logger::Severity.constants.each do |name|
      define_method(name.downcase){ |*args| } # no-op
    end
  end

  TimeoutError = Class.new(RuntimeError)

  # this error should never be "swallowed", if it is caught be sure to re-raise
  # it so the workers shutdown; otherwise workers will get killed
  # (`Thread#kill`) by ruby which can cause a problems
  ShutdownError = Class.new(Interrupt)

end
