require 'logger'

require 'dat-worker-pool/version'
require 'dat-worker-pool/queue'
require 'dat-worker-pool/runner'
require 'dat-worker-pool/worker'

class DatWorkerPool

  DEFAULT_NUM_WORKERS = 1
  MIN_WORKERS         = 1

  attr_reader :queue

  def initialize(worker_class, options = nil)
    if !worker_class.kind_of?(Class) || !worker_class.include?(DatWorkerPool::Worker)
      raise ArgumentError, "worker class must include `#{DatWorkerPool::Worker}`"
    end

    options ||= {}
    num_workers = (options[:num_workers] || DEFAULT_NUM_WORKERS).to_i
    if num_workers < MIN_WORKERS
      raise ArgumentError, "number of workers must be at least #{MIN_WORKERS}"
    end

    @queue = options[:queue] || begin
      require 'dat-worker-pool/default_queue'
      DatWorkerPool::DefaultQueue.new
    end

    @runner = DatWorkerPool::Runner.new({
      :num_workers   => num_workers,
      :logger        => options[:logger],
      :queue         => @queue,
      :worker_class  => worker_class,
      :worker_params => options[:worker_params]
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
    @queue.dwp_push work_item
  end
  alias :push :add_work

  def work_items
    @queue.work_items
  end

  def available_worker_count
    @runner.available_worker_count
  end

  def worker_available?
    @runner.worker_available?
  end

  # this error should never be "swallowed", if it is caught be sure to re-raise
  # it so the workers shutdown; otherwise workers will get killed
  # (`Thread#kill`) by ruby which can cause a problems
  ShutdownError = Class.new(Interrupt)

end
