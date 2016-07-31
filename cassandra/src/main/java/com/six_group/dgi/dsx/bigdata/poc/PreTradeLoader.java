package com.six_group.dgi.dsx.bigdata.poc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.six_group.dgi.dsx.bigdata.poc.parsing.ExtractValueUtil;
import com.six_group.dgi.dsx.bigdata.poc.parsing.FavFileMessageRunnable;
import com.six_group.dgi.dsx.bigdata.poc.parsing.SecurityPartitioner;
import com.six_group.dgi.dsx.bigdata.poc.quoting.QuoteBooks;
import com.six_group.dgi.dsx.bigdata.poc.quoting.Spread;
import com.six_group.dgi.dsx.bigdata.poc.quoting.Trade;

public class PreTradeLoader {
	private final QuoteBooks _books = new QuoteBooks();
	private final MonitorRunnable _monitorRunnable = new MonitorRunnable();
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
	private Thread _monitorThread;
	private int _lineCount = 0;
	private long _startTime;
	private long _stopTime;
	private final Date _currentDate;
	
	public static void main(String[] args) {
		Cluster cluster = null;
		try {
		    cluster = Cluster.builder().addContactPoint(args[0]).build();
		    final Session session = cluster.connect();
			final int threadCount = Integer.valueOf(args[1]); 
			final int batchSize = Integer.valueOf(args[2]);
			int timeOffsetInDays = 0;
			int timeOffsetInHours = 0;
			BigDecimal spreadVariant = BigDecimal.ZERO;
			boolean generateTables = false;
			if (args.length > 3) {
				timeOffsetInDays = Integer.valueOf(args[3]); 
			}
			if (args.length > 4) {
				timeOffsetInHours = Integer.valueOf(args[4]); 
			}
			if (args.length > 5) {
				generateTables = Boolean.valueOf(args[5]); 
			}
			if (args.length > 6) {
				spreadVariant = new BigDecimal(args[6]); 
			}
	        final PreTradeLoader loader = new PreTradeLoader(session, threadCount, batchSize, timeOffsetInDays, timeOffsetInHours, generateTables, spreadVariant);
	        loader.loadFavFile("/home/rbattenfeld/Documents/demo.fav.txt");
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
		printCassandraVersion();
	}	
	
	private void printCassandraVersion() {
		final ResultSet rs = _session.execute("select release_version from system.local");
	    final Row row = rs.one();
	    System.out.println("Connected to cassandra. Version:" + row.getString("Release_version"));
	}

	public void loadFavFile(final String pathToFile) {
		final int maxLinesPushed = _batchSize * _threadCount * 10;
		_startTime = System.currentTimeMillis();
		initBucketThreads();
		_monitorThread = new Thread(_monitorRunnable);
		_monitorThread.start();
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
			_books.printBookStatistic();
			System.out.println("Total Processing Time [ms]: " + (_stopTime - _startTime));
		} catch (final IOException ex) {
			ex.printStackTrace();
		} finally {
			_monitorRunnable.stop();	
			joinAllThreads();
		}
	}

	//-----------------------------------------------------------------------||
	//-- Private Methods ----------------------------------------------------||
	//-----------------------------------------------------------------------||

	private void generateTable() {
		_session.execute("DROP KEYSPACE IF EXISTS swx");
		_session.execute("CREATE KEYSPACE swx WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
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
	    		+ "origdate TIMESTAMP, "
	    		+ "origtime TIMESTAMP, "
	    		+ "price DECIMAL, "
	    		+ "volume BIGINT, "
	    		+ "PRIMARY KEY ((security, origdate), origtime, id))");
	    
		_session.execute("CREATE TABLE swx.spread ("
	    		+ "security TEXT, "
	    		+ "id BIGINT, "
	    		+ "house TEXT, "
	    		+ "origdate TIMESTAMP, "
	    		+ "origtime TIMESTAMP, "
                + "ask DECIMAL,"
                + "bid DECIMAL,"
                + "spread DECIMAL,"
                + "bestask DECIMAL,"
                + "bestbid DECIMAL,"
	    		+ "PRIMARY KEY ((security, origdate), origtime, id))");
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
	    	final FavFileMessageRunnable runnable = new FavFileMessageRunnable(_mapperSpread, _mapperTrade, _books, _batchSize, _currentDate, _timeOffsetInHours, _spreadVariant);
	    	_messageProcessors.add(runnable);
	    	_executorService.execute(runnable);
	    }
	}
	
	private void joinAllThreads() {
		for (final FavFileMessageRunnable runnable : _messageProcessors) {
			runnable.stop();
	    }
		try {
			_executorService.awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		}
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
				_books.printBookStatistic();
				return;
			} else {
				ExtractValueUtil.INSTANCE.sleep(500);
			}
		}
	}
	
	private class MonitorRunnable implements Runnable {
		private AtomicBoolean _isStopped = new AtomicBoolean(false);
		
		public void stop() {
			_isStopped.set(true);
		}
		
		@Override
		public void run() {
			while (!_isStopped.get()) {
				for (int i = 0; i < _threadCount; i++) {
					System.out.println("Queue[" + i + "]: " + _messageProcessors.get(i).getQueueSize());
			    }
				ExtractValueUtil.INSTANCE.sleep(5000);
			}		
		}		
	}
}
