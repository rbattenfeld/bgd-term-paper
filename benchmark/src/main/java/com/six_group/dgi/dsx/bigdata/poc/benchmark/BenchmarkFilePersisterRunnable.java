package com.six_group.dgi.dsx.bigdata.poc.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.mapping.Mapper;

public class BenchmarkFilePersisterRunnable implements Runnable {
	private final int _batchSize;
	private final AtomicBoolean _isStopped = new AtomicBoolean(false);
	private final BlockingQueue<Benchmark> _lineQueue = new ArrayBlockingQueue<>(10000000);
	private final Mapper<Benchmark> _mapperBenchmark;
	private AtomicLong _benchmarkCount = new AtomicLong(0);

	public BenchmarkFilePersisterRunnable(final int batchSize, final Mapper<Benchmark> mapperBenchmark) {
		_batchSize = batchSize;
		_mapperBenchmark = mapperBenchmark;
    }

	public void offer(final Benchmark line) {
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
			    final Benchmark line = _lineQueue.poll(200, TimeUnit.MILLISECONDS);
				if (line != null) {
					_benchmarkCount.incrementAndGet();
					try {
						benchmarkList.add(line);
						if (benchmarkList.size() % _batchSize == 0) {
							saveSpreadsCassandra(benchmarkList);
							benchmarkList = new ArrayList<Benchmark>(); 
						}
					} catch (final Exception ex) {
						ex.printStackTrace();
						System.out.println("invalid row" + line);
					}
				} 
            }
		    saveSpreadsCassandra(benchmarkList);
			System.out.println("Stopped BenchmarkFilePersisterRunnable consumer");
		} catch (Exception ex) {
			ex.printStackTrace();
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