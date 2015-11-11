require 'logger'

require 'dat-worker-pool/version'
require 'dat-worker-pool/queue'
require 'dat-worker-pool/runner'
require 'dat-worker-pool/worker'

class DatWorkerPool

  DEFAULT_NUM_WORKERS = 1
  MIN_WORKERS         = 1

  attr_reader :logger, :queue

  def initialize(worker_class, options = nil)
    if !worker_class.kind_of?(Class) || !worker_class.include?(DatWorkerPool::Worker)
      raise ArgumentError, "worker class must include `#{DatWorkerPool::Worker}`"
    end

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

    @runner = DatWorkerPool::Runner.new({
      :num_workers   => num_workers,
      :queue         => @queue,
      :worker_class  => worker_class,
      :worker_params => options[:worker_params]
    })
  end

  def start
    @runner.start
  end

  def shutdown(timeout = nil)
    @runner.shutdown(timeout, caller)
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

  class NullLogger
    ::Logger::Severity.constants.each do |name|
      define_method(name.downcase){ |*args| } # no-op
    end
  end

  # this error should never be "swallowed", if it is caught be sure to re-raise
  # it so the workers shutdown; otherwise workers will get killed
  # (`Thread#kill`) by ruby which can cause a problems
  ShutdownError = Class.new(Interrupt)

end
