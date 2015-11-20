require 'dat-worker-pool/worker'

class DatWorkerPool

  class WorkerPoolSpy

    attr_reader :logger, :queue
    attr_reader :options, :num_workers, :worker_class, :worker_params
    attr_reader :start_called, :shutdown_called, :shutdown_timeout
    attr_accessor :available_worker_count, :worker_available

    def initialize(worker_class, options = nil)
      @worker_class = worker_class
      if !@worker_class.kind_of?(Class) || !@worker_class.include?(DatWorkerPool::Worker)
        raise ArgumentError, "worker class must include `#{DatWorkerPool::Worker}`"
      end

      @options      = options || {}
      @num_workers  = (@options[:num_workers] || DEFAULT_NUM_WORKERS).to_i
      if @num_workers && @num_workers < MIN_WORKERS
        raise ArgumentError, "number of workers must be at least #{MIN_WORKERS}"
      end

      @queue = @options[:queue] || begin
        require 'dat-worker-pool/default_queue'
        DatWorkerPool::DefaultQueue.new
      end

      @logger        = @options[:logger]
      @worker_params = @options[:worker_params]

      @available_worker_count = 0
      @worker_available       = false
      @start_called           = false
      @shutdown_called        = false
      @shutdown_timeout       = nil
    end

    def start
      @start_called = true
      @queue.dwp_start
    end

    def shutdown(timeout = nil)
      @shutdown_called  = true
      @shutdown_timeout = timeout
      @queue.dwp_shutdown
    end

    def add_work(work_item)
      return if work_item.nil?
      @queue.dwp_push(work_item)
    end
    alias :push :add_work

    def work_items
      @queue.work_items
    end

    def worker_available?
      !!@worker_available
    end

  end

end
