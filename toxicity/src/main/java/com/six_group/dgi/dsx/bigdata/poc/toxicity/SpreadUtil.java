package com.six_group.dgi.dsx.bigdata.poc.toxicity;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

public enum SpreadUtil {
	INSTANCE;
		
	public Double calculateBps(final Double tradePrice, final Double bucketPrice, final long factor) {
		if (bucketPrice != null) {
			return BigDecimal.valueOf(((tradePrice - bucketPrice) / tradePrice * factor)).setScale(5, RoundingMode.HALF_EVEN).doubleValue();
		} else {
			return null;
		}
	}
	
	public DailyEntireSpread getCeilingTrade(final Long keyTime, final TreeMap<Long, List<DailyEntireSpread>> spreadMultiMap) {
		long startTime = keyTime;
		while (true) {
			final Long key = spreadMultiMap.ceilingKey(startTime);
			if (key != null) {
				final List<DailyEntireSpread> t = spreadMultiMap.get(key);
				for (final DailyEntireSpread spread : t) {
					return spread;
				}
				startTime++;
			} else {
				return null;
			}
		}
	}

	public DailyEntireSpread getFloorTrade(final Long keyTime, final TreeMap<Long, List<DailyEntireSpread>> spreadMultiMap) {
		long startTime = keyTime;
		while (true) {
			final Long key = spreadMultiMap.floorKey(startTime);
			if (key != null) {
				final List<DailyEntireSpread> t = spreadMultiMap.get(key);
				for (final DailyEntireSpread spread : t) {
					return spread;
				}
				startTime--;
			} else {
				return null;
			}
		}
	}

	public Double getBpsBestBidFloorPrice(final Calendar cal, final TradeFact trade, final TreeMap<Long, List<DailyEntireSpread>> spreadMultiMap, final int time, final int maxTime) {
		final Date tradeTimestamp = trade.getTradedTimestamp();
		final long minTimeMS = rollMilliseconds(cal, tradeTimestamp, time).getTime();
		final long maxTimeMS = rollMilliseconds(cal, tradeTimestamp, maxTime).getTime();
		final DailyEntireSpread currentSpread = getFloorTrade(rollMilliseconds(cal, tradeTimestamp, time).getTime(), spreadMultiMap);
		if (currentSpread != null && currentSpread.getLastBestBidPrice() != null) {
			return currentSpread.getLastBestBidPrice();
		} 
		return null;
	}
	
	public Double getBpsBestBidCeilingPrice(final Calendar cal, final TradeFact trade, final TreeMap<Long, List<DailyEntireSpread>> spreadMultiMap, final int time, final int maxTime) {
		final Date tradeTimestamp = trade.getTradedTimestamp();
		final long minTimeMS = rollMilliseconds(cal, tradeTimestamp, time).getTime();
		final DailyEntireSpread currentSpread = getCeilingTrade(minTimeMS, spreadMultiMap);
		if (currentSpread != null && currentSpread.getLastBestBidPrice() != null) {
			return currentSpread.getLastBestBidPrice();
		} 
		return null;
	}
	
	public Double getBpsBestAskFloorPrice(final Calendar cal, final TradeFact trade, final TreeMap<Long, List<DailyEntireSpread>> spreadMultiMap, final int time, final int maxTime) {
		final Date tradeTimestamp = trade.getTradedTimestamp();
		final DailyEntireSpread currentSpread = getFloorTrade(rollMilliseconds(cal, tradeTimestamp, time).getTime(), spreadMultiMap);
		if (currentSpread != null && currentSpread.getLastBestAskPrice() != null) {
			return currentSpread.getLastBestAskPrice();
		} 
		return null;
	}
	
	public Double getBpsBestAskCeilingPrice(final Calendar cal, final TradeFact trade, final TreeMap<Long, List<DailyEntireSpread>> spreadMultiMap, final int time, final int maxTime) {
		final Date tradeTimestamp = trade.getTradedTimestamp();
		final DailyEntireSpread currentSpread = getCeilingTrade(rollMilliseconds(cal, tradeTimestamp, time).getTime(), spreadMultiMap);
		if (currentSpread != null && currentSpread.getLastBestAskPrice() != null) {
			return currentSpread.getLastBestAskPrice();
		} 
		return null;
	}
	
	public Date rollMilliseconds(final Calendar cal, final Date timestamp, final int value) {
		cal.setTime(timestamp);
	    cal.add(Calendar.MILLISECOND, value);
	    return cal.getTime();
	}
	
	public void sleep(final long timout) {
		Thread.currentThread();
        try {
			Thread.sleep(timout);
		} catch (InterruptedException ex) {
			throw new RuntimeException(ex);
		}
	}
}
