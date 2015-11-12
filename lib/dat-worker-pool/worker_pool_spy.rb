class DatWorkerPool

  class WorkerPoolSpy

    attr_reader :logger, :queue
    attr_reader :options, :num_workers, :worker_class, :worker_params
    attr_reader :work_items
    attr_reader :start_called, :shutdown_called, :shutdown_timeout
    attr_accessor :worker_available

    def initialize(worker_class, options = nil)
      @worker_class = worker_class
      if !@worker_class.kind_of?(Class) || !@worker_class.include?(DatWorkerPool::Worker)
        raise ArgumentError, "worker class must include `#{DatWorkerPool::Worker}`"
      end

      @options      = options
      @num_workers  = @options[:num_workers].to_i
      if @num_workers && @num_workers < MIN_WORKERS
        raise ArgumentError, "number of workers must be at least #{MIN_WORKERS}"
      end

      @logger        = @options[:logger]
      @queue         = @options[:queue]
      @worker_params = @options[:worker_params]

      @worker_available = false
      @work_items       = []
      @start_called     = false
      @shutdown_called  = false
      @shutdown_timeout = nil
    end

    def start
      @start_called = true
    end

    def shutdown(timeout = nil)
      @shutdown_called  = true
      @shutdown_timeout = timeout
    end

    def add_work(work_item)
      return if work_item.nil?
      @work_items << work_item
    end

    def waiting
      @num_workers
    end

    def worker_available?
      !!@worker_available
    end

  end

end
