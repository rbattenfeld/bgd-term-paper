package com.six_group.dgi.dsx.bigdata.poc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;

public class PreTradeLoader {
	private final QuoteBooks _books = new QuoteBooks();
	private final MonitorRunnable _monitorRunnable = new MonitorRunnable();
	private final int _threadCount;
	private final int _batchSize;
	private final JavaSparkContext _sc;
	private final Map<String, Integer> _securityToBucketMap = new HashMap<>();
	private final int _timeOffsetInDays;
	private final int _timeOffsetInHours;
	private final boolean _generateTables;
	private final BigDecimal _spreadVariant;
	private BlockingQueue<String>[] _lineQueues;
	private FavFileMessageRunnable[] _messageProcessors;
	private Thread[] _msgProcessorThreads;
	private Thread _monitorThread;
	private int _lineCount = 0;
	private int _currentBucketPointer = -1;
	private long _startTime;
	private long _stopTime;
	private final Date _currentDate;
	
	public static void main(String[] args) {  
		final int threadCount = Integer.valueOf(args[2]); 
		final int batchSize = Integer.valueOf(args[3]);
		int timeOffsetInDays = 0;
		int timeOffsetInHours = 0;
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
        final PreTradeLoader loader = new PreTradeLoader(getSparkConf(args), threadCount, batchSize, timeOffsetInDays, timeOffsetInHours,generateTables, spreadVariant);
    } 

	private static SparkConf getSparkConf(String[] args) {
		final SparkConf conf = new SparkConf();
        conf.setAppName("Pre Trade Loader");
        conf.setMaster(args[0]);
        conf.set("spark.cassandra.connection.host", args[1]);
        return conf;
	}
	
	public PreTradeLoader(final SparkConf conf, final int threads, final int batches, final int timeOffsetInDays, final int timeOffsetInHours, final boolean generateTables,final BigDecimal spreadVariant) {
		_threadCount = threads;
		_batchSize = batches;
		_timeOffsetInDays = timeOffsetInDays;
		_timeOffsetInHours = timeOffsetInHours;
		_generateTables = generateTables;
		_spreadVariant = spreadVariant;
		_currentDate = addDays(new Date(), _timeOffsetInDays);
		_sc = new JavaSparkContext(conf);
		if (generateTables) {
			generateTable();
		}
		loadFavFile("/home/rbattenfeld/Documents/demo.fav.txt");
	}	
	
	private void generateTable() {
		final CassandraConnector connector = CassandraConnector.apply(_sc.getConf());    	 
    	try (Session session = connector.openSession()) {
    	    session.execute("DROP KEYSPACE IF EXISTS swx");
    	    session.execute("CREATE KEYSPACE swx WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    	    session.execute("CREATE TABLE swx.trade ("
    	    		+ "security TEXT, " 
    	    		+ "id BIGINT, "
    	    		+ " bidId BIGINT, "
    	    		+ " askId BIGINT, "
    	    		+ " askLiquidityInd TEXT, "
    	    		+ " bidLiquidityInd TEXT, "	
    	    		+ " askMemberOrg TEXT, "
    	    		+ " bidMemberOrg TEXT, "	
    	    		+ " askHouse TEXT, "
    	    		+ " bidHouse TEXT, "	
    	    		+ " origdate TIMESTAMP, "
    	    		+ " origtime TIMESTAMP, "
    	    		+ " price DECIMAL, "
    	    		+ " volume BIGINT, "
    	    		+ "PRIMARY KEY ((security), origdate, origtime, id))");
    	    
    	    session.execute("CREATE TABLE swx.spread ("
    	    		+ "security TEXT, "
    	    		+ "id TEXT, "
    	    		+ "house TEXT, "
    	    		+ "origdate TIMESTAMP, "
    	    		+ "origtime TIMESTAMP, "
                    + "ask DECIMAL,"
                    + "bid DECIMAL,"
                    + "spread DECIMAL,"
                    + "bestask DECIMAL,"
                    + "bestbid DECIMAL,"
    	    		+ "PRIMARY KEY ((security), origdate, house, origtime, id))");
    	}
    }

	public void loadFavFile(final String pathToFile) {
		final int maxLinesPushed = _batchSize * _threadCount * 2;
		_startTime = System.currentTimeMillis();
		initBucketThreads(_sc);
		_monitorThread = new Thread(_monitorRunnable);
		_monitorThread.start();
		try (final BufferedReader br = new BufferedReader(new FileReader(pathToFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				_lineCount++;
				final String security = getSecurity(line);
				if (security != null) {
					_lineQueues[getBucket(security)].offer(line);  // TODO offer loop
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
			for (int i = 0; i < _threadCount; i++) {
		    	_messageProcessors[i].stop();
		    }
		}
	}
	
	private String getSecurity(final String line) {
		if (line.contains("security")) {
			return ExtractValueUtil.INSTANCE.getValue(line, "security", true);
		} else {
			return null;
		}
	}
	
	private int getBucket(final String security) {
		final Integer bucketNo = _securityToBucketMap.get(security);
		if (bucketNo != null) {
			return bucketNo;
		} else {
			_currentBucketPointer++;
			if (_currentBucketPointer >= _threadCount) {
				_currentBucketPointer = 0;
			}
			_securityToBucketMap.put(security, _currentBucketPointer);
			return _currentBucketPointer;
		}
	}

	private void initBucketThreads(final JavaSparkContext sc) {
		_lineQueues = new ArrayBlockingQueue[_threadCount];
		_messageProcessors = new FavFileMessageRunnable[_threadCount];
		_msgProcessorThreads = new Thread[_threadCount];
	    for (int i = 0; i < _threadCount; i++) {
	    	_lineQueues[i] = new ArrayBlockingQueue<String>(10000000);
	    	_messageProcessors[i] = new FavFileMessageRunnable(sc, _books, _lineQueues[i], _batchSize, _currentDate, _timeOffsetInHours, _spreadVariant);
	    	_msgProcessorThreads[i] = new Thread(_messageProcessors[i]);
	    	_msgProcessorThreads[i].start();
	    }
	}
	
	private void waitForCompletition(final int lowerBound) {
		System.out.println("----- waitForCompletition (lineCount read: " + _lineCount + ")");
		while (true) {
			int totalQueuesSize = 0;
			for (int i = 0; i < _threadCount; i++) {
				totalQueuesSize += _lineQueues[i].size();
				System.out.println("Queue[" + i + "]: " + _lineQueues[i].size());
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
	
	private static Date addDays(final Date date, final int days) {
        final GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
        cal.add(Calendar.DATE, days);                 
        return cal.getTime();
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
					System.out.println("Queue[" + i + "]: " + _lineQueues[i].size());
			    }
				ExtractValueUtil.INSTANCE.sleep(5000);
			}		
		}		
	}
}
