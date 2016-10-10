package com.six_group.dgi.dsx.bigdata.poc.toxicity;

import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Month;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Query;

public class QuoteAnalyser {
	private static final Logger _Logger = Logger.getLogger(QuoteAnalyser.class.getName()); 
	private static final String _SELECT_INSTRUMENT = "SELECT * FROM swx.mrtadmin_instrument_dim WHERE instrument_dim_key IN ?;";
	private static final String _SELECT_TRADE_FACT = "SELECT * FROM swx.mrtadmin_trade_fact WHERE traded_date_dim_key=%d";
	private final PropertiesHandler _propHandler;
	private final Session _session;
	private final MappingManager _mapperManager;
	private final Mapper<TradeFact> _mapperTrade;
	private final Mapper<DailyEntireSpread> _mapperSpread;
	private final Mapper<Instrument> _mapperInstrument;
	private final Mapper<Participant> _mapperParticipant;
	private final Mapper<Environment> _mapperEnvironment;
	private final Mapper<TradingBehaviour> _mapperTradingBehaviour;
	private final Mapper<TradingVenue> _mapperVenue;
	private final Mapper<Toxicity> _mapperToxicity;
	private final PreparedStatement _preparedStmInstrument;
	private final ConcurrentMap <Long, Instrument> _instrumentIsinMap = new ConcurrentHashMap<>();
	private final ConcurrentMap <Long, Participant> _participantMap = new ConcurrentHashMap<>();
	private final ConcurrentMap<String, Participant> _participantMapByAbbrev = new ConcurrentHashMap<>();	
	private final ConcurrentMap <Long, Environment> _environmentMap = new ConcurrentHashMap<>();
	private final ConcurrentMap <Long, TradingBehaviour> _tradingBehaviourMap = new ConcurrentHashMap<>();
	private final ConcurrentMap <Long, TradingVenue> _tradingVenueMap = new ConcurrentHashMap<>();
	private final ConcurrentMap <Long, SecUniverse> _secUniverseMap = new ConcurrentHashMap<>();
	private final ConcurrentMap <Integer, List<SpreadSorter>> _spreadSorterMap = new ConcurrentHashMap<>();
	private final ExecutorService _executorService;
	private final AtomicLong _tradeCounter = new AtomicLong(0);
	private final String _tradingSegmentListStr;
	private final List<Long> _tradingSegmentList;
	private final List<Long> _memberIDList = new ArrayList<>();
	
	public static void main(String[] args) {
		Cluster cluster = null;
		try {
			final PropertiesHandler propHandler = PropertiesHandler.load(args[0]);
			final Integer monthToLoad = Integer.valueOf(args[1]);
			final String tradingSegmentList = args[2];
			final String memberIDString = args[3];
			final Boolean isETLOnly = Boolean.valueOf(args[4]);
			final String[] cassandraHosts = propHandler.getStringValue(PropertiesHandler.KEY_CASSANDRA_HOSTS).split(",", -1);
			final int cassandraPort = propHandler.getIntValue(PropertiesHandler.KEY_CASSANDRA_PORT);
			final int threadCount = propHandler.getIntValue(PropertiesHandler.KEY_THREAD_POOL);
			final int maxRequestsPerConnection = propHandler.getIntValue(PropertiesHandler.KEY_REQ_PER_CONN);
			final int maxConnections = threadCount / maxRequestsPerConnection + 1;
			final PoolingOptions pools = new PoolingOptions();
	        pools.setNewConnectionThreshold(HostDistance.LOCAL, threadCount);
	        pools.setCoreConnectionsPerHost(HostDistance.LOCAL, maxConnections);
	        pools.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnections);
	        pools.setCoreConnectionsPerHost(HostDistance.REMOTE, maxConnections);
	        pools.setMaxConnectionsPerHost(HostDistance.REMOTE, maxConnections);   
	        cluster = Cluster.builder().addContactPoints(cassandraHosts)
	                .withPort(cassandraPort)
	        		.withPoolingOptions(pools)
                    .withSocketOptions(new SocketOptions().setTcpNoDelay(true))
                    .build();
		    final Session session = cluster.connect();	
		    loadDriver();
			printCassandraVersion(session, cluster.getMetadata());
	        new QuoteAnalyser(session, propHandler, memberIDString, tradingSegmentList, monthToLoad, isETLOnly);
		} catch (final Exception ex) {
			_Logger.log(Level.SEVERE, ex.getMessage(), ex);
	    } finally {
		    if (cluster != null) {
		    	cluster.close();
		    }
		}
    } 
	
	public QuoteAnalyser(final Session session, final PropertiesHandler propHandler, final String memberIDString, final String tradingSegmentListStr, final Integer monthToLoad, final boolean isETLOnly) {
		_propHandler = propHandler;
		_executorService = Executors.newFixedThreadPool(_propHandler.getIntValue(PropertiesHandler.KEY_THREAD_POOL));
		try {
			_session = session;
			_mapperManager = new MappingManager(_session);
			_mapperSpread = _mapperManager.mapper(DailyEntireSpread.class);
			_mapperTrade = _mapperManager.mapper(TradeFact.class);
			_mapperInstrument = _mapperManager.mapper(Instrument.class);		
			_mapperParticipant = _mapperManager.mapper(Participant.class);
			_mapperTradingBehaviour = _mapperManager.mapper(TradingBehaviour.class);
			_mapperToxicity = _mapperManager.mapper(Toxicity.class);
			_mapperEnvironment = _mapperManager.mapper(Environment.class);
			_mapperVenue = _mapperManager.mapper(TradingVenue.class);
			_tradingSegmentListStr = tradingSegmentListStr;
			_tradingSegmentList = getTradingSegmentList(_tradingSegmentListStr);
			_preparedStmInstrument = _session.prepare(_SELECT_INSTRUMENT);
			fetchAllParticipants();
			fetchTradingBehaviour();
			fetchEnvironments();
			fetchVenues();
			if (isETLOnly) {
				loadParticipants(monthToLoad);     // TODO
				loadTradingBehaviour(monthToLoad); // TODO
				loadEnvironments(monthToLoad);     // TODO
				loadVenues(monthToLoad);           // TODO
				loadProducts(monthToLoad);
				loadInstruments(monthToLoad);
				loadTradeFact(monthToLoad);
				loadSpread(monthToLoad);
			} else {
				startSpreadFetcher(monthToLoad, false);
				startSpreadFetcher(monthToLoad, true);
				processMemberIDs(memberIDString);
				for (int day = 1; day < 33; day++) {
					final long currentDateAsLong = (monthToLoad * 100) + day;
					final long startTime = System.currentTimeMillis();
					final List<SpreadSorter> spreadList = getSpreads(day);
					final TradeListHandler tradeListHandler = new TradeListHandler(spreadList, currentDateAsLong);
					final ResultSet resultsTradeFact = session.execute(String.format(_SELECT_TRADE_FACT, currentDateAsLong));
					final Result<TradeFact> trades = _mapperTrade.map(resultsTradeFact);
					Long tradeCounter = 0L;
					for (final TradeFact trade : trades) {
						tradeCounter++;
						if (trades.getAvailableWithoutFetching() < 2000 && !trades.isFullyFetched()) {
							trades.fetchMoreResults();
						}					
						if (_memberIDList.contains(trade.getBuyParticipantDimKey())) {
							tradeListHandler.put(trade.getBuyParticipantDimKey(), trade);
						}					
						if (_memberIDList.contains(trade.getSellParticipantDimKey())) {
							tradeListHandler.put(trade.getSellParticipantDimKey(), trade);
						}
					}
					waitUntilAllRunningBocksFinished();
					tradeListHandler.cleanUp();
					waitUntilAllRunningBocksFinished();
					final long processingTimeBusinessDay = System.currentTimeMillis() - startTime;
					_Logger.info(String.format("Processing business day with trades: %d spreads: %d time[ms]: %d", tradeCounter, spreadList.size(), processingTimeBusinessDay));
					_spreadSorterMap.remove(day);
				}
			}
		} catch (final Exception ex) {
			throw new RuntimeException(ex);
		} finally {
			if (_executorService != null) {
				_executorService.shutdown();
			}
		}
	}	

	//-----------------------------------------------------------------------||
	//-- Private Methods ----------------------------------------------------||
	//-----------------------------------------------------------------------||
	
	private void processTradeList(final Long memberID, final long currentDateAsLong, final List<TradeFact> tradeList, final List<SpreadSorter> spreadSortedList) {
		try {
			fetchInstruments(tradeList);
			TreeMap<Long, List<DailyEntireSpread>> spreadMultiMap = null;
			Date previousTradeTimestamp = null;
			String previousIsin = null;
			final Calendar cal = Calendar.getInstance();
			final List<Toxicity> toxicityList = new ArrayList<>();
			for (final TradeFact trade : tradeList) {		
				final Participant buyParticipant = _participantMap.get(trade.getBuyParticipantDimKey());
				final Participant sellParticipant = _participantMap.get(trade.getSellParticipantDimKey());
				final TradingBehaviour buyBehaviour = _tradingBehaviourMap.get(trade.getBuyTradingBehaviourDimKey());
				final TradingBehaviour sellBehaviour = _tradingBehaviourMap.get(trade.getSellTradingBehaviourDimKey());
				final Environment environment = _environmentMap.get(trade.getTradingEnvironmentDimKey());
				final Date tradeTimestamp = trade.getTradedTimestamp();
				final TradingVenue venue = _tradingVenueMap.get(trade.getTradingVenueDimKey());
				if  (buyBehaviour != null && !buyBehaviour.getLiquidityCode().equals("AU") 
				 && sellBehaviour != null && !sellBehaviour.getLiquidityCode().equals("AU")
				 && environment.getTradingPlace().equals("SIX")
				 && environment.getTradingExchange().equals("On Exchange")
				 && environment.getReversalStatus().equals("Not Reversed")
				 && venue != null && ("XSWX".equals(venue.getTradingVenueCode()) || "XVTX".equals(venue.getTradingVenueCode()))) {
					final Instrument instrument = _instrumentIsinMap.get(trade.getInstrumentDimKey());
					String side = null;
					String posterAggressor = null;					
					if (instrument != null && instrument.getIsin() != null) {					
						spreadMultiMap = getSpreadSlice(cal, previousIsin, instrument.getIsin(), previousTradeTimestamp, tradeTimestamp, spreadSortedList, spreadMultiMap);
						if (!spreadMultiMap.isEmpty()) {
							previousTradeTimestamp = tradeTimestamp;
							previousIsin = instrument.getIsin();										
							if (buyParticipant != null && buyParticipant.getParticipantDimKey().equals(memberID)) {
								side = ToxicityCalculator._BUY;
								posterAggressor = buyBehaviour.getLiquidityCode();
							} else if (sellParticipant != null && sellParticipant.getParticipantDimKey().equals(memberID)) {
								side = ToxicityCalculator._SELL;
								posterAggressor = sellBehaviour.getLiquidityCode();
							} else {
								_Logger.info("unkown");
								continue;
							}							
							final Toxicity toxicity = ToxicityCalculator.calculate(cal, memberID, currentDateAsLong, trade, instrument, spreadMultiMap, side, posterAggressor);
							toxicity.setTradingSegList(_tradingSegmentListStr);
							toxicity.setParticipantAbbrev((_participantMap.get(memberID)).getParticipantAbbrev());
							toxicityList.add(toxicity);
							if (toxicityList.size() % 100 == 0) {
								saveToxicityList(toxicityList);
							}
						}
					} else {
						_Logger.warning("Instrument not found. InstrumentDimKey: %d" + trade.getInstrumentDimKey());
					}
				} else {
					_Logger.log(Level.FINEST, String.format("%s %s %s %s %s %s", 
							environment.getTradingPlace(), 
							environment.getTradingExchange(), 
							environment.getReversalStatus(), 
							venue.getTradingVenueCode(), 
							buyBehaviour.getLiquidityCode(), 
							sellBehaviour.getLiquidityCode()));
				}
			}
			saveToxicityList(toxicityList);
			_Logger.info(String.format("Saved memberID: %d records: %d", memberID, tradeList.size()));
		} catch (final Exception ex) {
			_Logger.log(Level.SEVERE, ex.getMessage(), ex);
		}
	}

	private void saveToxicityList(final List<Toxicity> toxicityList) { 
        if (toxicityList.size() > 0) {
            final BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
            for (final Toxicity toxicity: toxicityList) {
                batch.add(_mapperToxicity.saveQuery(toxicity));
            }
            for (int retry = 0; retry < 3; retry++) {
	            try {
	            	_mapperToxicity.getManager().getSession().execute(batch);
	            	return;
	            } catch (final com.datastax.driver.core.exceptions.WriteTimeoutException ex) {
	            	_Logger.log(Level.WARNING, ex.getMessage());
	            }
	            SpreadUtil.INSTANCE.sleep(1000);
            }
        }
    }

	private void saveTradeList(final List<TradeFact> tradeList) { 
        if (tradeList.size() > 0) {
            final BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
            for (final TradeFact trade: tradeList) {
                batch.add(_mapperTrade.saveQuery(trade));
            }
            _mapperTrade.getManager().getSession().execute(batch);
        }
    }

	private void saveSpreadList(final List<DailyEntireSpread> spreadList) { 
        if (spreadList.size() > 0) {
            final BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
            for (final DailyEntireSpread spread: spreadList) {
                batch.add(_mapperSpread.saveQuery(spread));
            }
            _mapperTrade.getManager().getSession().execute(batch);
        }
    }

	private void saveInstrumentList(final List<Instrument> instrumentList) { 
        if (instrumentList.size() > 0) {
            final BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
            for (final Instrument instrument: instrumentList) {
                batch.add(_mapperInstrument.saveQuery(instrument));
            }
            _mapperInstrument.getManager().getSession().execute(batch);
        }
    }
	
	private void loadParticipants(final Integer monthToLoad) {
	}

	private void loadTradingBehaviour(final Integer monthToLoad) {
	}

	private void loadEnvironments(final Integer monthToLoad) {
	}

	private void loadVenues(final Integer monthToLoad) {
	}
	
	private void loadProducts(final Integer monthToLoad) {
		_Logger.info(String.format("Fetching products from : %d ...", monthToLoad));
		final long startFetchTime = System.currentTimeMillis();
		final int month = monthToLoad % 100;
		final int year = monthToLoad / 100;
		final Month monthObj = Month.of(month);
		final LocalDate startDate = LocalDate.of(year, month, 1);
		final LocalDate endDate = LocalDate.of(year, month, monthObj.maxLength());
		final String startDateStr = startDate.format(DateTimeFormatter.ofPattern("dd.MM.yyyy", Locale.getDefault()));
		final String endDateStr = endDate.format(DateTimeFormatter.ofPattern("dd.MM.yyyy", Locale.getDefault()));
		try (final Connection conn = DriverManager.getConnection(_propHandler.getStringValue(PropertiesHandler.KEY_ORACLE_CONN), _propHandler.getStringValue(PropertiesHandler.KEY_ORACLE_USER), _propHandler.getStringValue(PropertiesHandler.KEY_ORACLE_PWD))) {
			int count = 0;		
			final String selectSQL = String.format(_propHandler.getStringValue(PropertiesHandler.KEY_SELECT_PROD), _tradingSegmentListStr, startDateStr, startDateStr, endDateStr);
			final java.sql.PreparedStatement preparedStatement = conn.prepareStatement(selectSQL);
			final java.sql.ResultSet rs = preparedStatement.executeQuery(selectSQL );
			while (rs.next()) {
				final SecUniverse universe = new SecUniverse();
				universe.setProductIsin(rs.getString("product_isin"));
				universe.setProductKey(rs.getLong("product_key"));
				universe.setProductSymbol(rs.getString("product_symbol"));
				universe.setTradingSegmentID(rs.getLong("trading_segment_id"));
				_secUniverseMap.putIfAbsent(universe.getProductKey(), universe);
				count++;
			}
			final long fetchTime = System.currentTimeMillis() - startFetchTime;
			_Logger.info(String.format("Fetching products from : %d done (rows: %d time[ms]: %d)", monthToLoad, count, fetchTime));
		} catch (Exception ex) {
			_Logger.log(Level.SEVERE, ex.getMessage(), ex);
		}
	}
	private void loadInstruments(final Integer monthToLoad) {
		_Logger.info(String.format("Fetching instruments from : %d ...", monthToLoad));
		final long startFetchTime = System.currentTimeMillis();
		final int month = monthToLoad % 100;
		final int year = monthToLoad / 100;
		final Month monthObj = Month.of(month);
		final LocalDate startDate = LocalDate.of(year, month, 1);
		final LocalDate endDate = LocalDate.of(year, month, monthObj.maxLength());
		final String startDateStr = startDate.format(DateTimeFormatter.ofPattern("dd.MM.yyyy", Locale.getDefault()));
		final String endDateStr = endDate.format(DateTimeFormatter.ofPattern("dd.MM.yyyy", Locale.getDefault()));
		try (final Connection conn = DriverManager.getConnection(_propHandler.getStringValue(PropertiesHandler.KEY_ORACLE_CONN), _propHandler.getStringValue(PropertiesHandler.KEY_ORACLE_USER), _propHandler.getStringValue(PropertiesHandler.KEY_ORACLE_PWD))) {
			int count = 0;		
			final String selectSQL = String.format(_propHandler.getStringValue(PropertiesHandler.KEY_SELECT_INSTR), startDateStr, endDateStr);
			final java.sql.PreparedStatement preparedStatement = conn.prepareStatement(selectSQL);
			final java.sql.ResultSet rs = preparedStatement.executeQuery(selectSQL );
			final List<Instrument> instrumentList = new ArrayList<>();
			while (rs.next()) {
				final Instrument instrument = new Instrument();
				instrument.setInstrumentDimKey(rs.getLong("instrument_dim_key"));
				instrument.setIsin(rs.getString("isin"));
				instrument.setSymbol(rs.getString("symbol"));
				instrument.setValor(rs.getString("valor"));
				instrument.setEffectiveStartDate(new Date(rs.getTimestamp("effective_start_date").getTime()));
				instrument.setEffectiveEndDate(new Date(rs.getTimestamp("effective_end_date").getTime()));
				instrumentList.add(instrument);
				count++;				
				if (instrumentList.size() % 100 == 0) {
					saveInstrumentList(instrumentList);
					instrumentList.clear();
				}				
			}
			saveInstrumentList(instrumentList);
			final long fetchTime = System.currentTimeMillis() - startFetchTime;
			_Logger.info(String.format("Fetching instruments from : %d done (rows: %d time[ms]: %d)", monthToLoad, count, fetchTime));
		} catch (Exception ex) {
			_Logger.log(Level.SEVERE, ex.getMessage(), ex);
		}
	}
	
	private void loadTradeFact(final Integer monthToLoad) {
		_Logger.info(String.format("Copying trades from : %d ...", monthToLoad));
		final long startFetchTime = System.currentTimeMillis();
		try (final Connection conn = DriverManager.getConnection(_propHandler.getStringValue(PropertiesHandler.KEY_ORACLE_CONN), _propHandler.getStringValue(PropertiesHandler.KEY_ORACLE_USER), _propHandler.getStringValue(PropertiesHandler.KEY_ORACLE_PWD))) {
			int count = 0;
			for (int day = 1; day < 33; day++) {
				final String selectSQL = String.format("SELECT * FROM mrtadmin.trade_fact WHERE traded_date_dim_key = %d", (monthToLoad * 100) + day);
				final java.sql.PreparedStatement preparedStatement = conn.prepareStatement(selectSQL);
				final java.sql.ResultSet rs = preparedStatement.executeQuery(selectSQL );
				final List<TradeFact> tradeList = new ArrayList<>();
				while (rs.next()) {
					final TradeFact trade = new TradeFact();
					trade.setPrice(rs.getBigDecimal("PRICE").doubleValue());
					trade.setVolume(rs.getLong("volume"));
					trade.setTradeID(rs.getString("trade_id"));
					trade.setTradedTimestamp(new Date(rs.getTimestamp("traded_timestamp").getTime()));
					trade.setBuyParticipantDimKey(rs.getLong("buy_participant_dim_key"));
					trade.setSellParticipantDimKey(rs.getLong("sell_participant_dim_key"));
					trade.setBuyTradingBehaviourDimKey(rs.getLong("buy_trading_behaviour_dim_key"));
					trade.setSellTradingBehaviourDimKey(rs.getLong("sell_trading_behaviour_dim_key"));
					trade.setTradedDateDimKey(rs.getLong("traded_date_dim_key"));
					trade.setInstrumentDimKey(rs.getLong("instrument_dim_key"));
					trade.setTradingEnvironmentDimKey(rs.getLong("trading_environment_dim_key"));
					trade.setTradingVenueDimKey(rs.getLong("trading_venue_dim_key"));
					tradeList.add(trade);						
					if (tradeList.size() % 100 == 0) {
						saveTradeList(tradeList);
						tradeList.clear();
					}
				}
				saveTradeList(tradeList);	
			}
			final long fetchTime = System.currentTimeMillis() - startFetchTime;
			_Logger.info(String.format("Copying trades from : %d done (rows: %d time[ms]: %d)", monthToLoad, count, fetchTime));
		} catch (Exception ex) {
			_Logger.log(Level.SEVERE, ex.getMessage(), ex);
		}
	}
	
	private void loadSpread(final Integer monthToLoad) {
		_Logger.info(String.format("Copying spreads from : %d ...", monthToLoad));
		final long startFetchTime = System.currentTimeMillis();
		final List<Future<?>> futureList = new ArrayList<>();
		try (final Connection conn = DriverManager.getConnection(_propHandler.getStringValue(PropertiesHandler.KEY_ORACLE_CONN), _propHandler.getStringValue(PropertiesHandler.KEY_ORACLE_USER), _propHandler.getStringValue(PropertiesHandler.KEY_ORACLE_PWD))) {
			int count = 0;
			for (int day = 1; day < 33; day++) {
				futureList.add(_executorService.submit(new SpreadCopyRunnable(monthToLoad, day)));
			}			
			int loopCount = 0;
			while (loopCount < 10000) {
				boolean isAllDone = true;
				for (Future<?> future : futureList) {
					if (!future.isDone()) {
						isAllDone = false;
					}
				}
				if (isAllDone) {
					break;
				} else {
					SpreadUtil.INSTANCE.sleep(2000);
				}
				loopCount++;
			}
			
			final long fetchTime = System.currentTimeMillis() - startFetchTime;
			_Logger.info(String.format("Copying spreads from : %d done (rows: %d time[ms]: %d)", monthToLoad, count, fetchTime));
		} catch (Exception ex) {
			_Logger.log(Level.SEVERE, ex.getMessage(), ex);
		} 
	}

	private void loadSpreadDay(final int monthToLoad, final Integer day) {
		_Logger.info(String.format("Copying spreads for day: %d ...", day));
		final long startFetchTime = System.currentTimeMillis();
		try (final Connection conn = DriverManager.getConnection(_propHandler.getStringValue(PropertiesHandler.KEY_ORACLE_CONN), _propHandler.getStringValue(PropertiesHandler.KEY_ORACLE_USER), _propHandler.getStringValue(PropertiesHandler.KEY_ORACLE_PWD))) {
			int count = 0;
			final String selectSQL = String.format("SELECT last_best_ask_price,last_best_ask_volume,last_best_bid_price,last_best_bid_volume,onbook_date_key,product_key,spread,transaction_timestamp FROM daily_entire_spread WHERE onbook_date_key = %d", (monthToLoad * 100) + day);
			final java.sql.PreparedStatement preparedStatement = conn.prepareStatement(selectSQL);
			final java.sql.ResultSet rs = preparedStatement.executeQuery(selectSQL );
			final List<DailyEntireSpread> spreadList = new ArrayList<>();
			while (rs.next()) {
				final DailyEntireSpread spread = new DailyEntireSpread();
				spread.setLastBestAskPrice(rs.getDouble("last_best_ask_price"));
				spread.setLastBestAskVolume(rs.getLong("last_best_ask_volume"));
				spread.setLastBestBidPrice(rs.getDouble("last_best_bid_price"));
				spread.setLastBestBidVolume(rs.getLong("last_best_bid_volume"));
				spread.setOnbookDateKey(rs.getLong("onbook_date_key"));
				spread.setProductKey(rs.getLong("product_key"));					
				final SecUniverse security = _secUniverseMap.get(spread.getProductKey());
				if (security != null) {
					spread.setProductIsin(_secUniverseMap.get(spread.getProductKey()).getProductIsin());
					spread.setTradingSegmentID(_secUniverseMap.get(spread.getProductKey()).getTradingSegmentID());
					spread.setSpread(rs.getDouble("spread"));
					spread.setTransactionTimestamp(new Date(rs.getTimestamp("transaction_timestamp").getTime()));
					spreadList.add(spread);
				} else {
					_Logger.log(Level.FINE, "Not Found ProductKey: " + spread.getProductKey());
				}
					
				if (spreadList.size() % 100 == 0) {
					saveSpreadList(spreadList);
					spreadList.clear();
				}
				count++;
			}
			saveSpreadList(spreadList);	
			final long fetchTime = System.currentTimeMillis() - startFetchTime;
			_Logger.info(String.format("Copying spreads for day: %d done (rows: %d time[ms]: %d)", day, count, fetchTime));
		} catch (Exception ex) {
			_Logger.log(Level.SEVERE, ex.getMessage(), ex);
		} 
	}
	
	private TreeMap<Long, List<DailyEntireSpread>> getSpreadSlice(final Calendar cal, final String oldIsin, final String newIsin, final Date oldTradeTime, final Date newTradeTime, final List<SpreadSorter> spreadSortedList, final TreeMap<Long, List<DailyEntireSpread>> spreadMultiMap) {
		if (oldIsin != null && oldIsin.equals(newIsin) && oldTradeTime != null && Math.abs(newTradeTime.getTime() - oldTradeTime.getTime()) < 5000) {
			return spreadMultiMap;
		} else {
			final Date before =  SpreadUtil.INSTANCE.rollMilliseconds(cal, newTradeTime, -20000);
			final Date after = SpreadUtil.INSTANCE.rollMilliseconds(cal, newTradeTime, 400000);
			return getSpreadsFromTo(spreadSortedList, newIsin, before.getTime(), after.getTime());
		}
	}

	private void printMap(TreeMap<Long, List<DailyEntireSpread>> map, final Date tradeTimestamp, final Double tradePrice) {
		SimpleDateFormat sdf = new SimpleDateFormat("dd-M-yyyy hh:mm:ss.SSS");
		for (Long key : map.keySet()) {
			final List<DailyEntireSpread> spreadList = map.get(key);
			for (DailyEntireSpread spread : spreadList) {
				System.out.println(String.format("Key: %d Spread: %s Trade: %s Diff: %d tradePrice; %f bidPrice: %f askPrice: %f", key, sdf.format(spread.getTransactionTimestamp()), sdf.format(tradeTimestamp), spread.getTransactionTimestamp().getTime() - tradeTimestamp.getTime(), spread.getLastBestBidPrice(), spread.getLastBestAskPrice(), tradePrice));
			}
			
		}
	}

	private void fetchInstruments(final List<TradeFact> tradeList) {
		final List<Long> instrumentDimList = new ArrayList<>();
		for (final TradeFact trade : tradeList) {
			if (!_instrumentIsinMap.containsKey(trade.getInstrumentDimKey())) {
				instrumentDimList.add(trade.getInstrumentDimKey());
			}
		}
		if (instrumentDimList.size() > 0) {
			final ResultSet resultsInstruments = _session.execute(_preparedStmInstrument.bind(instrumentDimList));
			final Result<Instrument> instruments = _mapperInstrument.map(resultsInstruments);
			for (Instrument instr : instruments) {
				_instrumentIsinMap.put(instr.getInstrumentDimKey(), instr);
			}
		}
	}
	
	private void fetchAllParticipants() {
		final ResultSet resultsParticpiants = _session.execute("SELECT * FROM swx.mrtadmin_participant_dim");
		final Result<Participant> participants = _mapperParticipant.map(resultsParticpiants);
		for (Participant participant : participants) {
			_participantMap.put(participant.getParticipantDimKey(), participant);
		}
	}
	
	private void fetchEnvironments() {
		final ResultSet resultsEnv = _session.execute("SELECT * FROM swx.mrtadmin_trading_environment_dim");
		final Result<Environment> envList = _mapperEnvironment.map(resultsEnv);
		for (Environment env : envList) {
			_environmentMap.put(env.getTradingEnvironmentDimKey(), env);
		}
	}

	private void fetchTradingBehaviour() {
		final ResultSet resultsTradingBehaviour = _session.execute("SELECT * FROM swx.mrtadmin_trading_behaviour_dim");
		final Result<TradingBehaviour> tradingBeahvours = _mapperTradingBehaviour.map(resultsTradingBehaviour);
		for (TradingBehaviour tradingBeahvour : tradingBeahvours) {
			_tradingBehaviourMap.put(tradingBeahvour.getTradingBehaviourDimKey(), tradingBeahvour);
		}
	}

	private void fetchVenues() {
		final ResultSet resultsParticpiants = _session.execute("SELECT * from swx.mrtadmin_trading_venue_dim");
		final Result<TradingVenue> venues = _mapperVenue.map(resultsParticpiants);
		for (TradingVenue venue : venues) {
			_tradingVenueMap.put(venue.getTradingVenueDimKey(), venue);
		}
	}
	
	private static void printCassandraVersion(final Session session, final Metadata metadata) {
		final ResultSet rs = session.execute("select release_version from system.local");
	    final Row row = rs.one();
	    _Logger.info(String.format("Connected to cluster '%s' on %s.", metadata.getClusterName(), metadata.getAllHosts())); 
	    _Logger.info("Running with cassandra version:" + row.getString("Release_version"));
	}

	@Accessor
	public interface SpreadAccessor {
	    @Query("SELECT * FROM swx.daily_entire_spread WHERE onbook_date_key=? AND transaction_timestamp>=? AND transaction_timestamp<=?")
	    Result<DailyEntireSpread> getAll(final Long tradedDate, final Date fromDate, final Date toDate);
	}
	
	private TreeMap<Long, List<DailyEntireSpread>> getSpreadsFromTo(final List<SpreadSorter> spreadSortedList, final String isin, final long fromTime, final long toTime) {
		final TreeMap<Long, List<DailyEntireSpread>> spreadMultiMap = new TreeMap<>();
		final DailyEntireSpread spreadA = new DailyEntireSpread();		
		final DailyEntireSpread spreadB = new DailyEntireSpread();
		spreadA.setTransactionTimestamp(new Date(fromTime));
		spreadB.setTransactionTimestamp(new Date(toTime));
		final SpreadSorter a = new SpreadSorter(spreadA);
		final SpreadSorter b = new SpreadSorter(spreadB);		
		final int start = Math.abs(Collections.binarySearch(spreadSortedList, a));	  // TODO Math.max
		final int end = Math.min(Math.abs(Collections.binarySearch(spreadSortedList, b)), spreadSortedList.size() - 1);		
		if (start < spreadSortedList.size() && end <= spreadSortedList.size()) {   // TODO Math.max
			final List<SpreadSorter> z = spreadSortedList.subList(Math.abs(start), Math.abs(end + 1));
			for (final SpreadSorter sorter : z) {
				if (isin.equals(sorter.getSpread().getProductIsin())) {
					if (_tradingSegmentList.contains(sorter.getSpread().getTradingSegmentID())) {				
						List<DailyEntireSpread> valueList = spreadMultiMap.get(sorter.getSpread().getTransactionTimestamp().getTime());
						if (valueList != null) {
							valueList.add(sorter.getSpread());
						} else {
							valueList = new ArrayList<>();
							valueList.add(sorter.getSpread());
							spreadMultiMap.put(sorter.getSpread().getTransactionTimestamp().getTime(), valueList);
						}
					}
				}
			}
		}
		return spreadMultiMap;
	}
	
	private List<SpreadSorter> getSpreads(final long currentDateAsLong) {
		_Logger.info(String.format("Loading spreads from : %d ...", currentDateAsLong));
		final long startFetchTime = System.currentTimeMillis();
		final List<SpreadSorter> spreadList = new ArrayList<>();
		final ResultSet resultsSpread = _session.execute(String.format("SELECT * FROM swx.daily_entire_spread WHERE onbook_date_key=%s", currentDateAsLong));
		final Result<DailyEntireSpread> spreads = _mapperSpread.map(resultsSpread);
		for (final DailyEntireSpread spread : spreads) {
			if (spreads.getAvailableWithoutFetching() < 10000 && !spreads.isFullyFetched()) {
				spreads.fetchMoreResults();
			}
			spreadList.add(new SpreadSorter(spread));
		}
		final long fetchTime = System.currentTimeMillis() - startFetchTime;
		_Logger.info(String.format("Loading spreads from : %d done (rows: %d time[ms]: %d)", currentDateAsLong, spreadList.size(), fetchTime));
		return spreadList;
	}
	
	private List<Long> getTradingSegmentList(final String tradingSegmentListStr) {
		final List<Long> segmentList = new ArrayList<>();
		final String[] items = tradingSegmentListStr.split(",", -1);
		for (final String item : items) {
			segmentList.add(Long.valueOf(item));
		}
		return segmentList;
	}
	
	private void processMemberIDs(final String memberIDsString) {
		final String[] items = memberIDsString.split(",", -1);
		for (final Participant participant : _participantMap.values()) {
			if (participant.getParticipantAbbrev() != null) {
				_participantMapByAbbrev.put(participant.getParticipantAbbrev(), participant);
			} else {
				_participantMapByAbbrev.put(participant.getParticipantShortName(), participant);
			}
		}		
		for (String item : items) {
			final Participant participant = _participantMapByAbbrev.get(item.trim());
			if (participant != null) {
				_memberIDList.add(participant.getParticipantDimKey());
			} else {
				_Logger.warning(String.format("Participant abbrev %s not found", item));
			}		
		}
	}

	private List<SpreadSorter> getSpreads(final int day) {
		final long sleeptTime = 200;
		final long maxWait = sleeptTime * 5 * 60 * 5;
		long count = 0;
		while (count < maxWait) {
			final List<SpreadSorter> spreads = _spreadSorterMap.get(day);
			if (spreads != null) {
				return spreads;
			} else {
				count += sleeptTime;
				SpreadUtil.INSTANCE.sleep(sleeptTime);
				_Logger.info(String.format("Waiting for spreads ... %d [ms]", count));
			}
		}
		throw new RuntimeException("Could not load spreads for the day: " + day);
	}

	private void startSpreadFetcher(final Integer monthToLoad, final boolean onlyEvenDays) {
		_executorService.execute(new SpreadLoadRunnable(monthToLoad, onlyEvenDays));	
	}
	
	private static void loadDriver() {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException ex) {
			throw new RuntimeException(ex);
		}
	}
	
	private class TradeListHandler {
		private final Map <Long, List<TradeFact>> _memberMap = new HashMap<>();
		private final List<SpreadSorter> _spreadList;			
		private final Long _currentDateAsLong;
		
		public TradeListHandler(final List<SpreadSorter> spreadList, final Long currentDateAsLong) {
			_spreadList = spreadList;
			_currentDateAsLong = currentDateAsLong;
		}

		public void put(final long memberID, final TradeFact trade) {
			List<TradeFact> tradeList =  _memberMap.get(memberID);
			if (tradeList != null) {
				tradeList.add(trade);
				if (tradeList.size() % 1000 == 0) {
					final TradeListProcessorRunnable runnable = new TradeListProcessorRunnable(memberID, _spreadList, tradeList, _currentDateAsLong);
					_memberMap.put(memberID, new ArrayList<>());
					_tradeCounter.incrementAndGet();
					_executorService.execute(runnable);
				}
			} else {
				tradeList = new ArrayList<>();
				tradeList.add(trade);
				_memberMap.put(memberID, tradeList);
			}
		}
		
		public void cleanUp() {
			for (final Long memberID : _memberMap.keySet()) {
				final List<TradeFact> tradeList =  _memberMap.get(memberID);
				final TradeListProcessorRunnable runnable = new TradeListProcessorRunnable(memberID, _spreadList, tradeList, _currentDateAsLong);
				_memberMap.put(memberID, new ArrayList<>());
				_tradeCounter.incrementAndGet();
				_executorService.execute(runnable);
			}
		}
	}
	
	private void waitUntilAllRunningBocksFinished() {
		while (_tradeCounter.get() > 0) {
			Thread.currentThread();
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				_Logger.log(Level.WARNING, e.getMessage(), e);
			}
		}	
	}

	private class TradeListProcessorRunnable implements Runnable {
		private final List<SpreadSorter> _spreadList;
		private final List<TradeFact> _tradeList;
		private final Long _currentDateAsLong;		
		private final Long _memberID;
		
		public TradeListProcessorRunnable(final Long memberID, final List<SpreadSorter> spreadList, final List<TradeFact> tradeList, final Long currentDateAsLong) {
			_spreadList = spreadList;
			_tradeList = tradeList;
			_currentDateAsLong = currentDateAsLong;
			_memberID = memberID;
		}
		
		@Override
		public void run() {
			processTradeList(_memberID, _currentDateAsLong, _tradeList, _spreadList);
			_tradeCounter.decrementAndGet();
		}		
	}
	
	private class SpreadLoadRunnable implements Runnable {
		private final Integer _monthToLoad;
		private final boolean _onlyEvenDays;
		
		public SpreadLoadRunnable(final Integer monthToLoad, final boolean onlyEvenDays) {
			_monthToLoad = monthToLoad;
			_onlyEvenDays = onlyEvenDays;
		}
		
		@Override
		public void run() {
			for (int day = 1; day < 33; day++) {
				if (_onlyEvenDays && (day % 2 == 0)) {
					loadDay(day);
				} else if (!_onlyEvenDays && (day % 2 != 0)) {
					loadDay(day);
				}
//				return; // test
			}
		}	
		
		private void loadDay(final int day) {
			final long currentDateAsLong = (_monthToLoad * 100) + day;
			final List<SpreadSorter> spreadList = getSpreads(currentDateAsLong);
			_spreadSorterMap.put(day, spreadList);
		}
	}

	private class SpreadCopyRunnable implements Runnable {
		private final Integer _monthToLoad;
		private final Integer _day;
		
		public SpreadCopyRunnable(final Integer monthToLoad, final Integer day) {
			_monthToLoad = monthToLoad;
			_day = day;
		}
		
		@Override
		public void run() {
			loadSpreadDay(_monthToLoad, _day);
		}	
	}
	
	private class SpreadSorter implements Comparable<SpreadSorter> {
		private final DailyEntireSpread _spread;

		public SpreadSorter(final DailyEntireSpread spread) {
			_spread = spread;
		}
		
		public DailyEntireSpread getSpread() {
			return _spread;
		}
		
		@Override
		public int compareTo(final SpreadSorter other) {
			return _spread.getTransactionTimestamp().compareTo(other.getSpread().getTransactionTimestamp());
		}		
	}
	
}
