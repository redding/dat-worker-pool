require 'logger'
require 'system_timer'
require 'thread'

require 'dat-worker-pool/version'
require 'dat-worker-pool/queue'
require 'dat-worker-pool/worker'

class DatWorkerPool

  TimeoutError = Class.new(RuntimeError)

  attr_reader :logger, :spawned

  def initialize(min = 0, max = 1, debug = false, &do_work_proc)
    @min_workers  = min
    @max_workers  = max
    @debug        = debug
    @logger       = Logger.new(@debug)
    @do_work_proc = do_work_proc

    @queue           = Queue.new
    @workers_waiting = WorkersWaiting.new

    @mutex   = Mutex.new
    @workers = []
    @spawned = 0

    @min_workers.times{ self.spawn_worker }
  end

  def work_items
    @queue.work_items
  end

  def waiting
    @workers_waiting.count
  end

  def worker_available?
    !reached_max_workers? || @workers_waiting.count > 0
  end

  def all_spawned_workers_are_busy?
    @workers_waiting.count <= 0
  end

  def reached_max_workers?
    @mutex.synchronize{ @spawned >= @max_workers }
  end

  def queue_empty?
    @queue.empty?
  end

  # Check if all workers are busy before adding the work. When the work is
  # added, a worker will stop waiting (if it was idle). Because of that, we
  # can't reliably check if all workers are busy. We might think all workers are
  # busy because we just woke up a sleeping worker to process this work. Then we
  # would spawn a worker to do nothing.
  def add_work(work_item)
    return if work_item.nil?
    new_worker_needed = all_spawned_workers_are_busy?
    @queue.push work_item
    self.spawn_worker if new_worker_needed && !reached_max_workers?
  end

  # Shutdown each worker and then the queue. Shutting down the queue will
  # signal any workers waiting on it to wake up, so they can start shutting
  # down. If a worker is processing work, then it will be joined and allowed to
  # finish.
  # **NOTE** Any work that is left on the queue isn't processed. The controlling
  # application for the worker pool should gracefully handle these items.
  def shutdown(timeout = nil)
    begin
      proc = OptionalTimeoutProc.new(timeout, true) do
        @workers.each(&:shutdown)
        @queue.shutdown

        # use this pattern instead of `each` -- we don't want to call `join` on
        # every worker (especially if they are shutting down on their own), we
        # just want to make sure that any who haven't had a chance to finish
        # get to (this is safe, otherwise you might get a dead thread in the
        # `each`).
        @workers.first.join until @workers.empty?
      end
      proc.call
    rescue TimeoutError => exception
      exception.message.replace "Timed out shutting down the worker pool"
      @debug ? raise(exception) : self.logger.error(exception.message)
    end
  end

  # public, because workers need to call it for themselves
  def despawn_worker(worker)
    @mutex.synchronize do
      @spawned -= 1
      @workers.delete worker
    end
  end

  protected

  def spawn_worker
    @mutex.synchronize do
      worker = Worker.new(self, @queue, @workers_waiting) do |work_item|
        do_work(work_item)
      end
      @workers << worker
      @spawned += 1
      worker
    end
  end

  def do_work(work_item)
    begin
      @do_work_proc.call(work_item)
    rescue Exception => exception
      self.logger.error "Exception raised while doing work!"
      self.logger.error "#{exception.class}: #{exception.message}"
      self.logger.error exception.backtrace.join("\n")
    end
  end

  class WorkersWaiting
    attr_reader :count

    def initialize
      @mutex = Mutex.new
      @count = 0
    end

    def increment
      @mutex.synchronize{ @count += 1 }
    end

    def decrement
      @mutex.synchronize{ @count -= 1 }
    end

  end

  class OptionalTimeoutProc
    def initialize(timeout, reraise = false, &proc)
      @timeout = timeout
      @reraise = reraise
      @proc    = proc
    end

    def call
      if @timeout
        begin
          SystemTimer.timeout(@timeout, TimeoutError, &@proc)
        rescue TimeoutError
          raise if @reraise
        end
      else
        @proc.call
      end
    end
  end

  module Logger
    def self.new(debug)
      debug ? ::Logger.new(STDOUT) : ::Logger.new(File.open('/dev/null', 'w'))
    end
  end

end
