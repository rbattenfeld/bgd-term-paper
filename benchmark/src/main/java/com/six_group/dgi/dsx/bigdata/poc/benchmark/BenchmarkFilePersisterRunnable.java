package com.six_group.dgi.dsx.bigdata.poc.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.mapping.Mapper;

public class BenchmarkFilePersisterRunnable implements Runnable {
	private static final Logger _Logger = Logger.getLogger(BenchmarkFilePersisterRunnable.class.getName()); 
	private final int _batchSize;
	private final AtomicBoolean _isStopped = new AtomicBoolean(false);
	private final BlockingQueue<BenchmarkRunnableItem> _lineQueue = new ArrayBlockingQueue<>(10000000);
	private final Mapper<Benchmark> _mapperBenchmark;
	private AtomicLong _benchmarkCount = new AtomicLong(0);

	public BenchmarkFilePersisterRunnable(final int batchSize, final Mapper<Benchmark> mapperBenchmark) {
		_batchSize = batchSize;
		_mapperBenchmark = mapperBenchmark;
    }

	public void offer(final BenchmarkRunnableItem line) {
		_lineQueue.offer(line);
	}
	
	public int getQueueSize() {
		return _lineQueue.size();
	}

    public long getBenchmarkCount() {
        return _benchmarkCount.get();
    }
    
	public void stop() {
		_isStopped.set(true);
	}
	
	@Override
	public void run() {
		try {	                    
            List<Benchmark> benchmarkList = new ArrayList<>(); 
            while (!_isStopped.get()) {
			    final BenchmarkRunnableItem runnableItem = _lineQueue.poll(200, TimeUnit.MILLISECONDS);
				if (runnableItem != null) {
					_benchmarkCount.incrementAndGet();
					try {
						final Benchmark benchmark = Benchmark.getBenchmark(runnableItem.getFileName(), runnableItem.getCurrentDateAsString(), runnableItem.getLine());
						if (benchmark.getTradeID() != null && !benchmark.getTradeID().isEmpty()) {
							benchmarkList.add(benchmark);
							if (benchmarkList.size() % _batchSize == 0) {
								saveSpreadsCassandra(benchmarkList);
								benchmarkList = new ArrayList<Benchmark>(); 
							}
						}
					} catch (final Exception ex) {
						_Logger.log(Level.SEVERE, "invalid row" + runnableItem.getLine(), ex);
					}
				} 
            }
		    saveSpreadsCassandra(benchmarkList);
		    _Logger.info("Stopped BenchmarkFilePersisterRunnable consumer");
		} catch (Exception ex) {
			_Logger.log(Level.SEVERE, ex.getMessage(), ex);
		}
	}

    private void saveSpreadsCassandra(final List<Benchmark> benchmarks) { 
        if (benchmarks.size() > 0) {
            final BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
            for (final Benchmark benchmark: benchmarks) {
                batch.add(_mapperBenchmark.saveQuery(benchmark));
            }
            _mapperBenchmark.getManager().getSession().execute(batch);
        }
    }
	
}