package com.six_group.dgi.dsx.bigdata.poc.benchmark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

public class BenchmarkFileLoader {
	private final int _threadCount;
	private final int _batchSize;
	private final Session _session;
	private final MappingManager _mapperManager;
	private final Mapper<Benchmark> _mapperBenchmark;
	private final int _timeOffsetInDays;
	private final ExecutorService _executorService;
	private final List<BenchmarkFilePersisterRunnable> _messageProcessors = new ArrayList<>();
	private final SecurityPartitioner _securityPartitioner;
	private int _lineCount = 0;
	private long _startTime;
	private long _stopTime;
	private final Date _currentDate;
	
	public static void main(String[] args) {
		Cluster cluster = null;
		try {
			final int threadCount = Integer.valueOf(args[1]); 
			final int batchSize = Integer.valueOf(args[2]);
			final String pathToBenchmarkFile = args[3];
			int timeOffsetInDays = 0;
            int cassandraPort = 9042;
			boolean generateTables = false;
			if (args.length > 4) {
				timeOffsetInDays = Integer.valueOf(args[4]); 
			}
			if (args.length > 5) {
				generateTables = Boolean.valueOf(args[5]); 
			}
            if (args.length > 6) {
                cassandraPort = Integer.valueOf(args[6]); 
            }
			final int maxRequestsPerConnection = 128;
			final int maxConnections = threadCount / maxRequestsPerConnection + 1;
			final PoolingOptions pools = new PoolingOptions();
	        pools.setNewConnectionThreshold(HostDistance.LOCAL, threadCount);
	        pools.setCoreConnectionsPerHost(HostDistance.LOCAL, maxConnections);
	        pools.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnections);
	        pools.setCoreConnectionsPerHost(HostDistance.REMOTE, maxConnections);
	        pools.setMaxConnectionsPerHost(HostDistance.REMOTE, maxConnections);	        
	        cluster = Cluster.builder().addContactPoint(args[0])
	                .withPort(cassandraPort)
	        		.withPoolingOptions(pools)
                    .withSocketOptions(new SocketOptions().setTcpNoDelay(true))
                    .build();
		    final Session session = cluster.connect();		    
			printCassandraVersion(session, cluster.getMetadata());
	        final BenchmarkFileLoader loader = new BenchmarkFileLoader(session, threadCount, batchSize, timeOffsetInDays, generateTables);
	        loader.loadFavFile(pathToBenchmarkFile);
		} finally {
		    if (cluster != null) {
		    	cluster.close();
		    }
		}
	}
	
	public BenchmarkFileLoader(final Session session, final int threads, final int batches, final int timeOffsetInDays, final boolean generateTables) {
		_session = session;
		_threadCount = threads;
		_batchSize = batches;
		_timeOffsetInDays = timeOffsetInDays;
		_currentDate = ExtractValueUtil.INSTANCE.addDays(new Date(), _timeOffsetInDays);
		_securityPartitioner = new SecurityPartitioner(_threadCount);
		_executorService = Executors.newFixedThreadPool(_threadCount);
		if (generateTables) {
			generateTable();
		}
		_mapperManager = new MappingManager(_session);
		_mapperBenchmark = _mapperManager.mapper(Benchmark.class);
	}

	public void loadFavFile(final String pathToFile) {
		final File benchmarkFile = new File(pathToFile);
		final String currentDateAsString = ExtractValueUtil.INSTANCE.getDateAsString(_currentDate);
		final int maxLinesPushed = _batchSize * _threadCount * 100;
		_startTime = System.currentTimeMillis();
		initBucketThreads();
		try (final BufferedReader br = new BufferedReader(new FileReader(pathToFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				_lineCount++;
				if (_lineCount > 1) {
					final Benchmark benchmark = Benchmark.getBenchmark(benchmarkFile.getName(), currentDateAsString, line);
					if (benchmark.getTradeID() != null && !benchmark.getTradeID().isEmpty()) {
						_messageProcessors.get(_securityPartitioner.getBucket(benchmark.getIsin())).offer(benchmark);				
						if (_lineCount % maxLinesPushed == 0) {
							waitForCompletition(maxLinesPushed / 2);
						}
					} else {
						System.out.println("Invalid record: " + benchmark);
					}
				}
			}
			waitForCompletition(0);
			_stopTime = System.currentTimeMillis();	
            final long processingTime = _stopTime - _startTime;
            final long totalBenchmarkCount = getTotalBenchmarkCount();
            final double msgPerSeconds = ((totalBenchmarkCount) * 1000.0) / processingTime;
			System.out.println(String.format("Current Day                : %s", _currentDate.toString()));
            System.out.println(String.format("Total benchmark records    : %d", totalBenchmarkCount));
			System.out.println(String.format("Total Processing Time [ms] : %d", _stopTime - _startTime));
            System.out.println(String.format("Average time [msg/s]       : %f", msgPerSeconds));
		} catch (final IOException ex) {
			ex.printStackTrace();
		} finally {
			joinAllThreads();
		}
	}
	
	//-----------------------------------------------------------------------||
	//-- Private Methods ----------------------------------------------------||
	//-----------------------------------------------------------------------||
	

	private void generateTable() {
		_session.execute("DROP TABLE IF EXISTS swx.benchmark");
		_session.execute("CREATE TABLE swx.benchmark ("				
				+ "businessDate TEXT, " 				
				+ "isin TEXT, " 
				+ "tradeID TEXT, " 				
				+ "fileName TEXT, " 
				+ "ccy TEXT, " 
				+ "fullName TEXT, " 
				+ "tradeTime TIMESTAMP, "
				+ "tradePrice DECIMAL, " 
				+ "tradeQty BIGINT, "
				+ "tidm TEXT, " 
				+ "side TEXT, " 
				+ "tradingNetworkID TEXT, " 
				+ "trader TEXT, " 
				+ "counterparty TEXT, " 
				+ "orderRef TEXT, " 
				+ "participant TEXT, " 
				+ "portfolioManagerID TEXT, "  
				+ "userDefined TEXT, "  
				+ "tradeTimeBenchmarkPriceTouch DECIMAL, "
				+ "hiLoBenchmarkPriceTouch DECIMAL, "
				+ "tradeTimeBenchmarkPriceEffective DECIMAL, " 
				+ "hiLoBenchmarkPriceEffective DECIMAL, "
				+ "tradeTimeBenchmarkVenueTouch TEXT, " 
				+ "hiLoBenchmarkVenueTouch TEXT, " 
				+ "tradeTimeBenchmarkVenueEffective TEXT, " 
				+ "hiLoBenchmarkVenueEffective TEXT, " 
				+ "tradeTimeBenchmarkBPSTouch TEXT, " 
				+ "hiLoBenchmarkBPSTouch TEXT, " 
				+ "tradeTimeBenchmarkBPsEffective TEXT, " 
				+ "hiLoBenchmarkBPSEffective TEXT, " 
				+ "tradeTimeBenchmarkShortfallValueTouch TEXT, " 
				+ "hiLoBenchmarkShortfallValueTouch TEXT, " 
				+ "tradeTimeBenchmarkShortfallValueEffective TEXT, " 
				+ "hiLoBenchmarkShortfallValueEffective TEXT, " 
				+ "xswxBestBidPrice DECIMAL, "
				+ "xswxBestBidVolume BIGINT, "
				+ "xswxBestOfferPrice DECIMAL, "
				+ "xswxBestOfferVolume BIGINT, "
				+ "xswxEffectiveBidPrice DECIMAL, "
				+ "xswxEffectiveOfferPrice DECIMAL, "
				+ "bateBestBidPrice DECIMAL, "
				+ "bateBestBidVolume BIGINT, "
				+ "bateBestOfferPrice DECIMAL, "
				+ "bateBestOfferVolume BIGINT, "
				+ "bateEffectiveBidPrice DECIMAL, "
				+ "bateEffectiveOfferPrice DECIMAL, "
				+ "chixBestBidPrice DECIMAL, "
				+ "chixBestBidVolume BIGINT, "
				+ "chixBestOfferPrice DECIMAL, "
				+ "chixBestOfferVolume BIGINT, "
				+ "chixEffectiveBidPrice DECIMAL, "
				+ "chixEffectiveOfferPrice DECIMAL, "
				+ "trqxBestBidPrice DECIMAL, "
				+ "trqxBestBidVolume BIGINT, "
			    + "trqxBestOfferPrice DECIMAL, "
				+ "trqxBestOfferVolume BIGINT, "
			    + "trqxEffectiveBidPrice DECIMAL, "
				+ "trqxEffectiveOfferPrice DECIMAL, "
	    		+ "PRIMARY KEY ((businessDate), isin, tradeID))");
    }
	
	private static void printCassandraVersion(final Session session, final Metadata metadata) {
		final ResultSet rs = session.execute("select release_version from system.local");
	    final Row row = rs.one();
	    System.out.println(String.format("Connected to cluster '%s' on %s.", metadata.getClusterName(), metadata.getAllHosts())); 
	    System.out.println("Running with cassandra version:" + row.getString("Release_version"));
	}
	
	private void initBucketThreads() {
	    for (int i = 0; i < _threadCount; i++) {
	    	final BenchmarkFilePersisterRunnable runnable = new BenchmarkFilePersisterRunnable(_batchSize, _mapperBenchmark);
	    	_messageProcessors.add(runnable);
	    	_executorService.execute(runnable);
	    }
	}
	
	private void joinAllThreads() {
		for (final BenchmarkFilePersisterRunnable runnable : _messageProcessors) {
			runnable.stop();
	    }
		 ExtractValueUtil.INSTANCE.sleep(5000);
         _executorService.shutdown();
	}

	private void waitForCompletition(final int lowerBound) {
		System.out.println("----- waitForCompletition (lineCount read: " + _lineCount + ")");
		while (true) {
			int totalQueuesSize = 0;
			for (int i = 0; i < _threadCount; i++) {
				totalQueuesSize += _messageProcessors.get(i).getQueueSize();
				System.out.println("Queue[" + i + "]: " + _messageProcessors.get(i).getQueueSize());
		    }		
			if (totalQueuesSize <= lowerBound) {
				System.out.println("----- waitForCompletition done-");
				return;
			} else {
				ExtractValueUtil.INSTANCE.sleep(50);
			}
		}
	}

	private long getTotalBenchmarkCount() {
	    long count = 0;
	    for (int i = 0; i < _threadCount; i++) {
	        count += _messageProcessors.get(i).getBenchmarkCount();
        }
	    return count;
	}
	
}
