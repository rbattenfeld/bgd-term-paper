package com.six_group.dgi.dsx.bigdata.poc;

import java.io.BufferedReader;
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
import com.six_group.dgi.dsx.bigdata.poc.parsing.ExtractValueUtil;
import com.six_group.dgi.dsx.bigdata.poc.parsing.FavFileMessageRunnable;
import com.six_group.dgi.dsx.bigdata.poc.parsing.SecurityPartitioner;
import com.six_group.dgi.dsx.bigdata.poc.persisting.CassandraPersister;
import com.six_group.dgi.dsx.bigdata.poc.quoting.QuoteBooks;
import com.six_group.dgi.dsx.bigdata.poc.quoting.Spread;
import com.six_group.dgi.dsx.bigdata.poc.quoting.Trade;

public class PreTradeLoader {
	private final QuoteBooks _books = new QuoteBooks();
	private final int _threadCount;
	private final int _batchSize;
	private final Session _session;
	private final MappingManager _mapperManager;
	private final Mapper<Trade> _mapperTrade;
	private final Mapper<Spread> _mapperSpread;
	private final int _timeOffsetInDays;
	private final int _timeOffsetInHours;
	private final List<FavFileMessageRunnable> _messageProcessors = new ArrayList<>();
	private final SecurityPartitioner _securityPartitioner;
	private final ExecutorService _executorService;
	private final BigDecimal _spreadVariant;
	private int _lineCount = 0;
	private long _startTime;
	private long _stopTime;
	private final Date _currentDate;
	
	public static void main(String[] args) {
		Cluster cluster = null;
		try {
			final int threadCount = Integer.valueOf(args[1]); 
			final int batchSize = Integer.valueOf(args[2]);
			int timeOffsetInDays = 0;
			int timeOffsetInHours = 0;
            int cassandraPort = 9042;
			BigDecimal spreadVariant = BigDecimal.ZERO;
			boolean generateTables = false;
			if (args.length > 4) {
				timeOffsetInDays = Integer.valueOf(args[4]); 
			}
			if (args.length > 5) {
				timeOffsetInHours = Integer.valueOf(args[5]); 
			}
			if (args.length > 6) {
				generateTables = Boolean.valueOf(args[6]); 
			}
			if (args.length > 7) {
				spreadVariant = new BigDecimal(args[7]); 
			}
            if (args.length > 8) {
                cassandraPort = Integer.valueOf(args[8]); 
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
	        final PreTradeLoader loader = new PreTradeLoader(session, threadCount, batchSize, timeOffsetInDays, timeOffsetInHours, generateTables, spreadVariant);
	        loader.loadFavFile(args[3]);
		} finally {
		    if (cluster != null) {
		    	cluster.close();
		    }
		}
    } 

	public PreTradeLoader(final Session session, final int threads, final int batches, final int timeOffsetInDays, final int timeOffsetInHours, final boolean generateTables,final BigDecimal spreadVariant) {
		_session = session;
		_threadCount = threads;
		_batchSize = batches;
		_timeOffsetInDays = timeOffsetInDays;
		_timeOffsetInHours = timeOffsetInHours;
		_spreadVariant = spreadVariant;
		_currentDate = ExtractValueUtil.INSTANCE.addDays(new Date(), _timeOffsetInDays);
		_securityPartitioner = new SecurityPartitioner(_threadCount);
		_executorService = Executors.newFixedThreadPool(_threadCount);
		if (generateTables) {
			generateTable();
		}
		_mapperManager = new MappingManager(_session);
		_mapperSpread = _mapperManager.mapper(Spread.class);
		_mapperTrade = _mapperManager.mapper(Trade.class);
//        _spreadUpdater = _mapperManager.createAccessor(SpreadUpdater.class);
	}	

	public void loadFavFile(final String pathToFile) {
		final int maxLinesPushed = _batchSize * _threadCount * 100;
		_startTime = System.currentTimeMillis();
		initBucketThreads();
		try (final BufferedReader br = new BufferedReader(new FileReader(pathToFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				_lineCount++;
				final String security = getSecurity(line);
				if (security != null) {
					_messageProcessors.get(_securityPartitioner.getBucket(security)).offer(line);
				}
				if (_lineCount % maxLinesPushed == 0) {
					waitForCompletition(maxLinesPushed / 2);
				}
			}
			waitForCompletition(0);
			_stopTime = System.currentTimeMillis();			
			final long totalOneSidedCount = getTotalOneSidedCount();
            final long totalTwoSidedCount = getTotalTwoSidedCount();
            final long totalTradeCount = getTotalTradeCount();
            final long processingTime = _stopTime - _startTime;
            final double msgPerSeconds = ((totalOneSidedCount + totalTwoSidedCount  + totalTradeCount) * 1000.0) / processingTime;
			System.out.println(String.format("Current Day                : %s", _currentDate.toString()));
            System.out.println(String.format("Total one sided legs       : %d", totalOneSidedCount));
            System.out.println(String.format("Total two sided legs       : %d", totalTwoSidedCount));
            System.out.println(String.format("Total delete messages      : %d", totalTradeCount));
			System.out.println(String.format("Total created trades       : %d", getTotalTradeCount()));
			System.out.println(String.format("Total Processing Time [ms] : %d", _stopTime - _startTime));
            System.out.println(String.format("Average time [msg/s]       : %f", msgPerSeconds));
            _books.printBookStatistic();
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
		_session.execute("DROP KEYSPACE IF EXISTS swx");
		_session.execute("CREATE KEYSPACE swx WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}");
		_session.execute("CREATE TABLE swx.trade ("
	    		+ "security TEXT, " 
	    		+ "id BIGINT, "
	    		+ "bidId BIGINT, "
	    		+ "askId BIGINT, "
	    		+ "askLiquidityInd TEXT, "
	    		+ "bidLiquidityInd TEXT, "	
	    		+ "askMemberOrg TEXT, "
	    		+ "bidMemberOrg TEXT, "	
	    		+ "askHouse TEXT, "
	    		+ "bidHouse TEXT, "	
	    		+ "origdate TEXT, "
	    		+ "origtime TIMESTAMP, "
	    		+ "price DECIMAL, "
	    		+ "volume BIGINT, "
	    		+ "PRIMARY KEY ((security, origdate), origtime, id))");
	    
		_session.execute("CREATE TABLE swx.spread ("
	    		+ "security TEXT, "
	    		+ "id BIGINT, "
	    		+ "house TEXT, "
                + "cur TEXT, "
                + "type TEXT, "
	    		+ "origdate TEXT, "
	    		+ "origtime TIMESTAMP, "
                + "deletetime TIMESTAMP, "
                + "ask DECIMAL,"
                + "bid DECIMAL,"
                + "spread DECIMAL,"
                + "bestask DECIMAL,"
                + "bestbid DECIMAL,"
                + "trade BOOLEAN,"
                + "price DECIMAL, "
                + "volume BIGINT, "
	    		+ "PRIMARY KEY ((security, origdate), type, trade, origtime, id))");
    }
	
	private String getSecurity(final String line) {
		if (line.contains("security")) {
			return ExtractValueUtil.INSTANCE.getValue(line, "security", true);
		} else {
			return null;
		}
	}

	private void initBucketThreads() {
	    for (int i = 0; i < _threadCount; i++) {
	        final CassandraPersister persistingService = new CassandraPersister(_mapperSpread, _mapperTrade);
	    	final FavFileMessageRunnable runnable = new FavFileMessageRunnable(persistingService, _books, _batchSize, _currentDate, _timeOffsetInHours, _spreadVariant);
	    	_messageProcessors.add(runnable);
	    	_executorService.execute(runnable);
	    }
	}
	
	private void joinAllThreads() {
		for (final FavFileMessageRunnable runnable : _messageProcessors) {
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

	private long getTotalTradeCount() {
	    long count = 0;
	    for (int i = 0; i < _threadCount; i++) {
	        count += _messageProcessors.get(i).getTradeCount();
        }
	    return count;
	}
	
    private long getTotalOneSidedCount() {
        long count = 0;
        for (int i = 0; i < _threadCount; i++) {
            count += _messageProcessors.get(i).getOneSidedCount();
        }
        return count;
    }

    private long getTotalTwoSidedCount() {
        long count = 0;
        for (int i = 0; i < _threadCount; i++) {
            count += _messageProcessors.get(i).getTwoSidedCount();
        }
        return count;
    }

    private long getTotalDeleteCount() {
        long count = 0;
        for (int i = 0; i < _threadCount; i++) {
            count += _messageProcessors.get(i).getDeleteCount();
        }
        return count;
    }
    
	private static void printCassandraVersion(final Session session, final Metadata metadata) {
		final ResultSet rs = session.execute("select release_version from system.local");
	    final Row row = rs.one();
	    System.out.println(String.format("Connected to cluster '%s' on %s.", metadata.getClusterName(), metadata.getAllHosts())); 
	    System.out.println("Running with cassandra version:" + row.getString("Release_version"));
	}
	
}
