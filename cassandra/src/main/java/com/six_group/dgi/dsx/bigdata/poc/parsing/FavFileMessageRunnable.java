package com.six_group.dgi.dsx.bigdata.poc.parsing;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.six_group.dgi.dsx.bigdata.poc.persisting.IPreTradePersister;
import com.six_group.dgi.dsx.bigdata.poc.quoting.QuoteBook;
import com.six_group.dgi.dsx.bigdata.poc.quoting.QuoteBooks;
import com.six_group.dgi.dsx.bigdata.poc.quoting.QuoteLeg;
import com.six_group.dgi.dsx.bigdata.poc.quoting.Spread;
import com.six_group.dgi.dsx.bigdata.poc.quoting.Trade;

public class FavFileMessageRunnable implements Runnable {
	private final QuoteBooks _books;
	private final IPreTradePersister _persitingService;
	private final int _batchSize;
	private final Date _currentDate;
	private final int _timeOffsetInHours;
	private final BigDecimal _spreadVariant;
	private final AtomicBoolean _isStopped = new AtomicBoolean(false);
	private final BlockingQueue<String> _lineQueue = new ArrayBlockingQueue<String>(10000000);
	private long _tradeCount = 0;
    private long _oneSidedCount = 0;
    private long _twoSidedCount = 0;
    private long _deleteCount = 0;

	public FavFileMessageRunnable(final IPreTradePersister persitingService, final QuoteBooks books, final int batchSize, final Date currentDate, final int timeOffsetInHours, final BigDecimal spreadVariant) {
	    _persitingService = persitingService;
		_books = books;
		_batchSize = batchSize;
		_currentDate = currentDate;
		_timeOffsetInHours = timeOffsetInHours;
		_spreadVariant = spreadVariant;
    }

	public void offer(final String line) {
		_lineQueue.offer(line);
	}
	
	public int getQueueSize() {
		return _lineQueue.size();
	}

    public long getTradeCount() {
        return _tradeCount;
    }

    public long getOneSidedCount() {
        return _oneSidedCount;
    }

    public long getTwoSidedCount() {
        return _twoSidedCount;
    }

    public long getDeleteCount() {
        return _deleteCount;
    }
    
	public void stop() {
		_isStopped.set(true);
	}
	
	@Override
	public void run() {
        final SimpleDateFormat timeFormatter1 = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss.S");
        final SimpleDateFormat timeFormatter2 = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
        final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
        final String currentDateAsStr = dateFormatter.format(_currentDate);
        final Calendar cal1 = Calendar.getInstance();
        final Calendar cal2 = Calendar.getInstance();
        cal2.setTime(_currentDate);
		try {	                    
            List<Spread> oneSidedList = new ArrayList<>(); 
			List<Spread> twoSidedList= new ArrayList<>();	        
			List<Trade> tradeList = new ArrayList<>();           
            List<Spread> deleteList = new ArrayList<>();
            final Map<String, Spread> twoSidedMap = new HashMap<>();
            final Map<Long, String> spreadKeyToBidIdMap = new HashMap<>();
			while (!_isStopped.get()) {
			    final String line = _lineQueue.poll(200, TimeUnit.MILLISECONDS);
				if (line != null) {
					try {
						if (line.contains("TRADE")) {
						    final Trade trade = getTrade(line, timeFormatter1, currentDateAsStr);
							tradeList.add(trade);
							updateTwoSidedWithTradeFlag(twoSidedMap, trade);
							if (tradeList.size() % _batchSize == 0) {
							    _tradeCount += tradeList.size();
							    _persitingService.saveTradesCassandra(tradeList);	
								tradeList = new ArrayList<>();
							}
						} else if (line.contains("ENTER")) {
					        final QuoteLeg leg = QuoteLeg.getLeg(line, timeFormatter1, timeFormatter2);
					        if (leg.getSide().equals(QuoteLeg.ASK)) {
                                leg.setPrice(leg.getPrice().add(_spreadVariant));
					        }		        
		 					_books.add(leg);
		 					if (leg.getOtherSide() != null) {
		 					    final String spreadKey = String.valueOf(leg.getBidLeg().getId()) + String.valueOf(leg.getAskLeg().getId());
		 						final Spread spread = new Spread(leg.getAskLeg(), leg.getBidLeg());
		 						spread.setOrigtime(ExtractValueUtil.INSTANCE.addHours(cal1, cal2, leg.getOrigtime(), _timeOffsetInHours));
		 						spread.setOrigdate(currentDateAsStr);
//		 						twoSidedList.add(spread);
		 						twoSidedMap.put(spreadKey, spread);
		 						spreadKeyToBidIdMap.put(leg.getBidLeg().getId(), spreadKey);
		 					}
						} else if (line.contains("DELET")) {
							final String security = ExtractValueUtil.INSTANCE.getValue(line, "security", true);
							final Long id = Long.valueOf(ExtractValueUtil.INSTANCE.getValue(line, " id", true));
							final Date origtime = timeFormatter1.parse("01.01.2016  " + ExtractValueUtil.INSTANCE.getTime(line));							
							final String spreadID = spreadKeyToBidIdMap.get(id);
							if (spreadID != null) {
							    final Spread twoSided = twoSidedMap.remove(spreadID);
    							if (twoSided != null) {
    							    twoSided.setDeletetime(ExtractValueUtil.INSTANCE.addHours(cal1, cal2, origtime, _timeOffsetInHours));
    							    twoSidedList.add(twoSided);
    							    if (twoSidedList.size() % _batchSize == 0) {
    		                             _twoSidedCount += twoSidedList.size();
    		                             _persitingService.saveSpreadsCassandra(twoSidedList);
    		                              twoSidedList = new ArrayList<>();
    		                        }
    							}
							}
							final QuoteBook book = _books.getBook(security);
							if (book != null) {
								final QuoteLeg leg = book.findQuote(id);
								if (leg != null) {
		                            _deleteCount++;
								    book.delete(id);
								    if (leg.getAskLeg() == null || leg.getBidLeg() == null) { 
								        final Spread oneSided = new Spread(leg);
								        oneSided.setOrigtime(ExtractValueUtil.INSTANCE.addHours(cal1, cal2, leg.getOrigtime(), _timeOffsetInHours));
								        oneSided.setOrigdate(currentDateAsStr);
								        oneSided.setDeletetime(ExtractValueUtil.INSTANCE.addHours(cal1, cal2, origtime, _timeOffsetInHours));
                                        oneSidedList.add(oneSided);
								        if (oneSidedList.size() % _batchSize == 0) {
								            _oneSidedCount += oneSidedList.size();
								            _persitingService.saveSpreadsCassandra(oneSidedList);
                                            oneSidedList = new ArrayList<>();
                                        }
								    }
								}
							}
						} else if (line.equals("EXIT")) {
						    break;
						}  
					} catch (final Exception ex) {
						ex.printStackTrace();
						System.out.println("invalid row" + line);
					}
				} 
			}
			_oneSidedCount += oneSidedList.size();
			_twoSidedCount += twoSidedList.size();
			_tradeCount += tradeList.size();
			_persitingService.saveSpreadsCassandra(oneSidedList);
			_persitingService.saveSpreadsCassandra(twoSidedList);
			_persitingService.saveSpreadsCassandra(deleteList);
			_persitingService.saveTradesCassandra(tradeList);
			deleteRemaingSpreads(twoSidedMap, cal1, cal2);
			System.out.println("Stopped FavFileMessageRunnable consumer");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private Trade getTrade(final String line, final SimpleDateFormat formatter, final String currentDateAsStr) throws ParseException {		
		final Trade trade = Trade.getTrade(line, _currentDate, _timeOffsetInHours, formatter);
		final QuoteBook book = _books.getBook(trade.getSecurity());
		if (book != null) {
			trade.setAskhouse(book.getAskHouse(trade.getAskid()));
			trade.setBidhouse(book.getAskHouse(trade.getBidid()));
		}
        trade.setOrigdate(currentDateAsStr);
		return trade;
	}
	
	private void deleteRemaingSpreads(final Map<String, Spread> twoSidedMap, final Calendar cal1, final Calendar cal2) {	    
	    List<Spread> twoSidedList= new ArrayList<>();
	    for (final Spread spread : twoSidedMap.values()) {
	        spread.setDeletetime(ExtractValueUtil.INSTANCE.zeroMinSecMilli(ExtractValueUtil.INSTANCE.addHours(cal1, cal2, spread.getOrigtime(), _timeOffsetInHours + 1)));
            twoSidedList.add(spread);
            if (twoSidedList.size() % _batchSize == 0) {
                 _twoSidedCount += twoSidedList.size();
                 _persitingService.saveSpreadsCassandra(twoSidedList);
                  twoSidedList = new ArrayList<>();
            }
	    }
	    _persitingService.saveSpreadsCassandra(twoSidedList);
	    _twoSidedCount += twoSidedMap.size();
	}

	private void updateTwoSidedWithTradeFlag(final Map<String, Spread> twoSidedMap, final Trade trade) {
	    final String spreadKey = String.valueOf(trade.getBidid()) + String.valueOf(trade.getAskid());
	    final Spread twoSided = twoSidedMap.get(spreadKey);
	    if (twoSided != null) {
	        twoSided.setTrade(true);
	        twoSided.setPrice(trade.getPrice());
	        twoSided.setVolume(trade.getVolume());
	    } else {
	        // TODO
	    }
	}
	
}