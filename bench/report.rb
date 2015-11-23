require 'benchmark'
require 'thread'

require 'dat-worker-pool'
require 'dat-worker-pool/default_queue'

class BenchRunner

  NUM_WORKERS   = 4
  TIME_MODIFIER = 10 ** 4 # 4 decimal places

  LOGGER = if ENV['DEBUG']
    Logger.new(File.expand_path("../../log/bench.log", __FILE__)).tap do |l|
      l.datetime_format = '' # don't show datetime in the logs
    end
  end

  def initialize
    output_file_path = if ENV['OUTPUT_FILE']
      File.expand_path(ENV['OUTPUT_FILE'])
    else
      File.expand_path('../report.txt', __FILE__)
    end
    @output = Output.new(File.open(output_file_path, 'w'))

    @number_of_work_items = ENV['NUM_WORK_ITEMS'] || 100_000

    @mutex    = Mutex.new
    @cond_var = ConditionVariable.new
    @finished = LockedInteger.new(0)

    @result = nil
  end

  def run
    output "Running benchmark report..."
    output("\n", false)

    benchmark_processing_work_items

    output "\n", false
    output "Processing #{@number_of_work_items} Work Items: #{@result}ms"

    output "\n"
    output "Done running benchmark report"
  end

  private

  def benchmark_processing_work_items
    queue = DatWorkerPool::DefaultQueue.new.tap(&:dwp_start)
    @number_of_work_items.times.each{ |n| queue.dwp_push(n + 1) }

    worker_pool = DatWorkerPool.new(BenchWorker, {
      :num_workers   => NUM_WORKERS,
      :logger        => LOGGER,
      :queue         => queue,
      :worker_params => {
        :mutex    => @mutex,
        :cond_var => @cond_var,
        :finished => @finished,
        :output   => @output
      }
    })
    benchmark = Benchmark.measure do
      worker_pool.start
      while @finished.value != @number_of_work_items
        @mutex.synchronize{ @cond_var.wait(@mutex) }
      end
      worker_pool.shutdown
    end

    @result = round_and_display(benchmark.real * 1000.to_f)
    output "\n", false
  end

  private

  def output(message, puts = true)
    @output.log(message, puts)
  end

  def round_and_display(time_in_ms)
    display_time(round_time(time_in_ms))
  end

  def round_time(time_in_ms)
    (time_in_ms * TIME_MODIFIER).to_i / TIME_MODIFIER.to_f
  end

  def display_time(time)
    integer, fractional = time.to_s.split('.')
    [integer, fractional.ljust(4, '0')].join('.')
  end

  class BenchWorker
    include DatWorkerPool::Worker

    on_available{ signal_main_thread }

    on_error{ params[:output].log('F', false) }

    def work!(n)
      params[:finished].increment
      params[:output].log('.', false) if ((n - 1) % 100 == 0)
    end

    private

    def signal_main_thread
      params[:mutex].synchronize{ params[:cond_var].signal }
    end
  end

  class Output < Struct.new(:file)
    def log(message, puts = true)
      method = puts ? :puts : :print
      self.send(method, message)
      self.file.send(method, message)
      STDOUT.flush if method == :print
    end
  end

  class LockedInteger < DatWorkerPool::LockedObject
    def increment; @mutex.synchronize{ @object = @object + 1 }; end
  end

end

BenchRunner.new.run
