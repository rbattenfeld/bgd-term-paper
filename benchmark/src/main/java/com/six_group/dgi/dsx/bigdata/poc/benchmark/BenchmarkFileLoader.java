package com.six_group.dgi.dsx.bigdata.poc.benchmark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

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
	private static final Logger _Logger = Logger.getLogger(BenchmarkFileLoader.class.getName()); 
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
	private long _processingTime;
	private final Date _currentDate;
	
	public static void main(String[] args) {
		Cluster cluster = null;
		try {
			final long startTime = System.currentTimeMillis();
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
			final List<String> files = fileList(pathToBenchmarkFile);
		    final Session session = cluster.connect();	
//			final List<Future<RunnableMetrics>> futureList = new ArrayList<>();
//		    final ExecutorService threadPoolExecutor = new ThreadPoolExecutor(threadCount, getMaxPoolSize(threadCount, files), 500, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());		    
			printCassandraVersion(session, cluster.getMetadata());
			
			final BenchmarkFileLoader loader = new BenchmarkFileLoader(session, threadCount, batchSize, 0, generateTables);
	        loader.loadBenchmarkFile(files);
			
//			for (final String pathToFile : files) {
//				final Future<RunnableMetrics> future = threadPoolExecutor.submit(new Callable<RunnableMetrics>() {
//					@Override
//					public RunnableMetrics call() throws Exception {
//						final BenchmarkFileLoader loader = new BenchmarkFileLoader(session, 5, batchSize, 0, false);
//				        loader.loadBenchmarkFile(pathToFile);
//				        return new RunnableMetrics(loader.getTotalBenchmarkCount(), loader.getProcessingTime());
//					}					
//				});
//				futureList.add(future);
//			}
//			while (true) {
//				boolean done = true;
//				for (final Future<?> future : futureList) {
//					if (!future.isDone()) {
//						done = false;
//					}
//				}
//				if (done) {
//					break;
//				} else {
//					ExtractValueUtil.INSTANCE.sleep(100);
//				}
//			}
//
//			final long stopTime = System.currentTimeMillis();
//			threadPoolExecutor.shutdown();
//			long totalRecords = 0;
//			long longestThreadProcessingTime = 0;
//			for (int i = 0; i < files.size(); i++) {
//				System.out.println(String.format("Loaded benchmark file : %s records: %d time: %d", files.get(i), futureList.get(i).get().getTotalRecordsProcessed(), futureList.get(i).get().getTotalProcessingTime()));
//				totalRecords += futureList.get(i).get().getTotalRecordsProcessed();
//				if (longestThreadProcessingTime < futureList.get(i).get().getTotalProcessingTime()) {
//					longestThreadProcessingTime = futureList.get(i).get().getTotalProcessingTime();
//				}
//			}
//            final double avgMsgPerSeconds = ((totalRecords) * 1000.0) / stopTime - startTime;
//			System.out.println(String.format("Total benchmark records    : %d", totalRecords));
//			System.out.println(String.format("Total Processing Time [ms] : %d", stopTime - startTime));
//            System.out.println(String.format("Average perf [msg/s]       : %f", avgMsgPerSeconds));
		} catch (final Exception ex) {
			_Logger.log(Level.SEVERE, ex.getMessage(), ex);
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

	public void loadBenchmarkFile(final List<String> pathToFileList) {
		try {
			final int maxLinesPushed = _batchSize * _threadCount * 100;
			_startTime = System.currentTimeMillis();
			initBucketThreads();
			for (final String pathToFile : pathToFileList) {
				final File benchmarkFile = new File(pathToFile);
				final String currentDateAsString = ExtractValueUtil.INSTANCE.getDateFromFileName(benchmarkFile.getName());
				try (final BufferedReader br = new BufferedReader(new FileReader(pathToFile))) {
					String line;
					long fileLineCount = 0;
					while ((line = br.readLine()) != null) {
						_lineCount++;
						fileLineCount++;
						if (fileLineCount > 1) {
							_messageProcessors.get(_securityPartitioner.getNextBucket()).offer(new BenchmarkRunnableItem(benchmarkFile.getName(), currentDateAsString, line));				
							if (_lineCount % maxLinesPushed == 0) {
								waitForCompletition(maxLinesPushed / 2);
							}
						}
					}
				} catch (final IOException ex) {
					_Logger.log(Level.SEVERE, ex.getMessage(), ex);
				}
			}
			waitForCompletition(0);
			_stopTime = System.currentTimeMillis();	
			_processingTime = _stopTime - _startTime;
            final long totalBenchmarkCount = getTotalBenchmarkCount();
            final double msgPerSeconds = ((totalBenchmarkCount) * 1000.0) / _processingTime;
			System.out.println(String.format("Current Day                : %s", _currentDate.toString()));
            System.out.println(String.format("Total benchmark records    : %d", totalBenchmarkCount));
			System.out.println(String.format("Total Processing Time [ms] : %d", _stopTime - _startTime));
            System.out.println(String.format("Average time [msg/s]       : %f", msgPerSeconds));
		} finally {
			joinAllThreads();
		}
	}
	
	public long getProcessingTime() {
		return _processingTime;
	}

	//-----------------------------------------------------------------------||
	//-- Private Methods ----------------------------------------------------||
	//-----------------------------------------------------------------------||
	

	private void generateTable() {
		_session.execute("DROP TABLE IF EXISTS swx.benchmark");
		_session.execute("CREATE TABLE swx.benchmark ("				
				+ "businessMonth TEXT, " 				
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
				+ "aqxeBestBidPrice DECIMAL, "
				+ "aqxeBestBidVolume BIGINT, "
			    + "aqxeBestOfferPrice DECIMAL, "
				+ "aqxeBestOfferVolume BIGINT, "
			    + "aqxeEffectiveBidPrice DECIMAL, "
				+ "aqxeEffectiveOfferPrice DECIMAL, "
                + "bestBidPriceXSWXDiffBATE DECIMAL, "
                + "bestBidPriceXSWXDiffCHIX DECIMAL, "
                + "bestBidPriceXSWXDiffTRQX DECIMAL, "	
                + "bestBidPriceXSWXDiffAQXE DECIMAL, "	    		
                + "spreadXswx DECIMAL, "
                + "spreadBate DECIMAL, "
                + "spreadChix DECIMAL, "
                + "spreadTrqx DECIMAL, "
                + "spreadAqxe DECIMAL, "
                + "highestBidPriceVenue TEXT, " 
                + "highestBidPriceRanking TEXT, " 
                + "lowestOfferPriceRanking TEXT, " 
	    		+ "PRIMARY KEY ((businessDate), isin, fileName))");
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
	
	private static List<String> fileList(final String pathToDirOrFile) {
		final List<String> fileNames = new ArrayList<>();
		final File pathTo = new File(pathToDirOrFile);
		if (pathTo.isDirectory()) {	        
	        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(pathToDirOrFile))) {
	            for (Path path : directoryStream) {
	                fileNames.add(path.toString());
	            }
	        } catch (IOException ex) {
	        	ex.printStackTrace();
	        }
		} else {
			fileNames.add(pathToDirOrFile);
		}
        return fileNames;
    }

}
