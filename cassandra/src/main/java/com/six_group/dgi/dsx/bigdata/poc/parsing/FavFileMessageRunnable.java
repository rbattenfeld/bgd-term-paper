package com.six_group.dgi.dsx.bigdata.poc.parsing;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.mapping.Mapper;
import com.six_group.dgi.dsx.bigdata.poc.quoting.QuoteBook;
import com.six_group.dgi.dsx.bigdata.poc.quoting.QuoteBooks;
import com.six_group.dgi.dsx.bigdata.poc.quoting.QuoteLeg;
import com.six_group.dgi.dsx.bigdata.poc.quoting.Spread;
import com.six_group.dgi.dsx.bigdata.poc.quoting.Trade;

public class FavFileMessageRunnable implements Runnable {
	private final QuoteBooks _books;
	private final Mapper<Spread> _mapperSpread;
	private final Mapper<Trade> _mapperTrade;
	private final int _batchSize;
	private final Date _currentDate;
	private final int _timeOffsetInHours;
	private final BigDecimal _spreadVariant;
	private final AtomicBoolean _isStopped = new AtomicBoolean(false);
	private final BlockingQueue<String> _lineQueue = new ArrayBlockingQueue<String>(10000000);

	public FavFileMessageRunnable(final Mapper<Spread> mapperSpread, Mapper<Trade> mapperTrade, final QuoteBooks books, final int batchSize, final Date currentDate, final int timeOffsetInHours, final BigDecimal spreadVariant) {
		_mapperSpread = mapperSpread;
		_mapperTrade = mapperTrade;
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
	
	public void stop() {
		_isStopped.set(true);
	}
	
	@Override
	public void run() {
        final SimpleDateFormat timeFormatter1 = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss.S");
        final SimpleDateFormat timeFormatter2 = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
        final Calendar cal1 = Calendar.getInstance();
        final Calendar cal2 = Calendar.getInstance();
        cal2.setTime(_currentDate);
		try {	        
			List<Spread> spreadList = new ArrayList<>();	        
			List<Trade> tradeList = new ArrayList<>();
			while (!_isStopped.get()) {
			    final String line = _lineQueue.poll(1, TimeUnit.SECONDS);
				if (line != null) {
					try {
						if (line.contains("TRADE")) {
							tradeList.add(getTrade(line, timeFormatter1));
							if (tradeList.size() % 1000 == 0) {
								saveTradesCassandra(tradeList);
								tradeList = new ArrayList<>();
							}
						} else if (line.contains("ENTER")) {
					        final QuoteLeg leg = QuoteLeg.getLeg(line, timeFormatter1, timeFormatter2);
					        if (leg.getSide().equals(QuoteLeg.ASK)) {
					        	leg.setPrice(leg.getPrice().add(_spreadVariant));
					        }
		 					_books.add(leg);
		 					if (leg.getOtherSide() != null) {
		 						final Spread spread = new Spread(leg.getAskLeg(), leg.getBidLeg());
		 						spread.setOrigtime(addHours(cal1, cal2, leg.getOrigtime(), _timeOffsetInHours));
		 						spread.setOrigdate(_currentDate);
		 						spreadList.add(spread);
		 					}
		 					if (spreadList.size() % _batchSize == 0) {
		 						saveSpreadsCassandra(spreadList);
		 						spreadList = new ArrayList<>();
		 					}
						} else if (line.contains("DELET")) {
							final String security = ExtractValueUtil.INSTANCE.getValue(line, "security", true);
							final Long id = Long.valueOf(ExtractValueUtil.INSTANCE.getValue(line, " id", true));
							final QuoteBook book = _books.getBook(security);
							if (book != null) {
								final QuoteLeg leg = book.findQuote(id);
								if (leg != null) {
								    book.delete(id);
								}
							}
						}				     
					} catch (final Exception ex) {
						ex.printStackTrace();
						System.out.println("invalid row" + line);
					}
				} 
			}
			saveSpreadsCassandra(spreadList);
			saveTradesCassandra(tradeList);
			System.out.println("Stopped FavFileMessageRunnable consumer");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private Trade getTrade(final String line, final SimpleDateFormat formatter) throws ParseException {		
		final Trade trade = Trade.getTrade(line, _currentDate, _timeOffsetInHours, formatter);
		final QuoteBook book = _books.getBook(trade.getSecurity());
		if (book != null) {
			trade.setAskhouse(book.getAskHouse(trade.getAskid()));
			trade.setBidhouse(book.getAskHouse(trade.getBidid()));
		}
		return trade;
	}
	
	private void saveSpreadsCassandra(final List<Spread> spreads) {	
		if (spreads.size() > 0) {
			final BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
			for (final Spread spread: spreads) {
				batch.add(_mapperSpread.saveQuery(spread));
			}
			_mapperSpread.getManager().getSession().execute(batch);
		}
	}
	
	private void saveTradesCassandra(final List<Trade> trades) {
		if (trades.size() > 0) {
			final BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
			for (final Trade trade: trades) {
				batch.add(_mapperTrade.saveQuery(trade));
			}
			_mapperSpread.getManager().getSession().execute(batch);
		}
	}
	
	private Date addDays(final Date date, final int days) {
        final GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.add(Calendar.DATE, days);                 
        return cal.getTime();
    }
	
	public Date addHours(final Calendar cal1, final Calendar cal2, final Date date, final int hours) {
        cal1.setTime(date);
        cal1.set(Calendar.YEAR, cal2.get(Calendar.YEAR));
        cal1.set(Calendar.MONTH, cal2.get(Calendar.MONTH));
        cal1.set(Calendar.DAY_OF_MONTH, cal2.get(Calendar.DAY_OF_MONTH));
        cal1.add(Calendar.HOUR, hours);                 
        return cal1.getTime();
    }
}