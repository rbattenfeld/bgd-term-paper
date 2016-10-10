package com.six_group.dgi.dsx.bigdata.poc.toxicity;

import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ToxicityCalculator {
	public static final String _AGGRESSOR = "AG";
	public static final String _POSTER = "PO";
	public static final String _BUY = "Buy";
	public static final String _SELL = "Sell";	
	private Double sell_AG_1000_Before = null;
	private Double sell_AG_500_Before = null;
	private Double sell_AG_100_Before = null;
	private Double sell_AG_50_After = null;
	private Double sell_AG_200_After = null;
	private Double sell_AG_500_After = null;
	private Double sell_AG_1000_After = null;
	private Double sell_AG_5000_After = null;
	private Double sell_AG_15000_After = null;
	private Double sell_AG_30000_After = null;
	private Double sell_AG_60000_After = null;
	private Double sell_AG_300000_After = null;	
	private Double sell_PO_1000_Before = null;
	private Double sell_PO_500_Before = null;
	private Double sell_PO_100_Before = null;
	private Double sell_PO_50_After = null;
	private Double sell_PO_200_After = null;
	private Double sell_PO_500_After = null;
	private Double sell_PO_1000_After = null;
	private Double sell_PO_5000_After = null;
	private Double sell_PO_15000_After = null;
	private Double sell_PO_30000_After = null;
	private Double sell_PO_60000_After = null;
	private Double sell_PO_300000_After = null;	
	private Double buy_AG_1000_Before = null;
	private Double buy_AG_500_Before = null;
	private Double buy_AG_100_Before = null;
	private Double buy_AG_50_After = null;
	private Double buy_AG_200_After = null;
	private Double buy_AG_500_After = null;
	private Double buy_AG_1000_After = null;
	private Double buy_AG_5000_After = null;
	private Double buy_AG_15000_After = null;
	private Double buy_AG_30000_After = null;
	private Double buy_AG_60000_After = null;
	private Double buy_AG_300000_After = null;	
	private Double buy_PO_1000_Before = null;
	private Double buy_PO_500_Before = null;
	private Double buy_PO_100_Before = null;
	private Double buy_PO_50_After = null;
	private Double buy_PO_200_After = null;
	private Double buy_PO_500_After = null;
	private Double buy_PO_1000_After = null;
	private Double buy_PO_5000_After = null;
	private Double buy_PO_15000_After = null;
	private Double buy_PO_30000_After = null;
	private Double buy_PO_60000_After = null;
	private Double buy_PO_300000_After = null;	

	public static Toxicity calculate(final Calendar cal, final long memberID, final long currentDateAsLong, final TradeFact trade, final Instrument instrument, TreeMap<Long, List<DailyEntireSpread>> spreadMultiMap, final String side, final String posterAggressor) {
		final ToxicityCalculator calculator = new ToxicityCalculator();
		return calculator.calc(cal, memberID, currentDateAsLong, trade, instrument, spreadMultiMap, side, posterAggressor);
	}
	
	//-----------------------------------------------------------------------||
	//-- Private Methods ----------------------------------------------------||
	//-----------------------------------------------------------------------||

	private ToxicityCalculator() {
	}

	private Toxicity calc(final Calendar cal, final long memberID, final long currentDateAsLong, final TradeFact trade, final Instrument instrument, TreeMap<Long, List<DailyEntireSpread>> spreadMultiMap, final String side, final String posterAggressor) {
		final Map<String, Double> bidAskPriceMap = findPrices(cal, trade, spreadMultiMap);
		if (side.equals(_SELL) && posterAggressor.equals(_AGGRESSOR)) {
			sell_AG_1000_Before  = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_1000_before"), 10000);
			sell_AG_500_Before   = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_500_before"), 10000);
			sell_AG_100_Before   = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_100_before"), 10000);
			sell_AG_50_After     = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_50_after"), 10000);
			sell_AG_200_After    = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_200_after"), 10000);
			sell_AG_500_After    = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_500_after"), 10000);
			sell_AG_1000_After   = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_1000_after"), 10000);
			sell_AG_5000_After   = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_5000_after"), 10000);
			sell_AG_15000_After  = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_15000_after"), 10000);
			sell_AG_30000_After  = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_30000_after"), 10000);
			sell_AG_60000_After  = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_60000_after"), 10000);
			sell_AG_300000_After = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_300000_after"), 10000);					
		} else if (side.equals(_SELL) && posterAggressor.equals(_POSTER)) {
			sell_PO_1000_Before  = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_1000_before"), 10000);
			sell_PO_500_Before   = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_500_before"), 10000);
			sell_PO_100_Before   = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_100_before"), 10000);
			sell_PO_50_After     = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_50_after"), 10000);
			sell_PO_200_After    = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_200_after"), 10000);
			sell_PO_500_After    = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_500_after"), 10000);
			sell_PO_1000_After   = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_1000_after"), 10000);
			sell_PO_5000_After   = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_5000_after"), 10000);
			sell_PO_15000_After  = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_15000_after"), 10000);
			sell_PO_30000_After  = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_30000_after"), 10000);
			sell_PO_60000_After  = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_60000_after"), 10000);
			sell_PO_300000_After = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_300000_after"), 10000);						
		} else if (side.equals(_BUY) && posterAggressor.equals(_AGGRESSOR)) {
			buy_AG_1000_Before   = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_1000_before"), -10000);			
			buy_AG_500_Before    = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_500_before"), -10000);
			buy_AG_100_Before    = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_100_before"), -10000);
			buy_AG_50_After      = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_50_after"), -10000);
			buy_AG_200_After     = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_200_after"), -10000);
			buy_AG_500_After     = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_500_after"), -10000);
			buy_AG_1000_After    = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_1000_after"), -10000);
			buy_AG_5000_After    = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_5000_after"), -10000);
			buy_AG_15000_After   = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_15000_after"), -10000);
			buy_AG_30000_After   = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_30000_after"), -10000);
			buy_AG_60000_After   = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_60000_after"), -10000);
			buy_AG_300000_After  = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("ask_300000_after"), -10000);		
		} else if (side.equals(_BUY) && posterAggressor.equals(_POSTER)) {
			buy_PO_1000_Before   = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_1000_before"), -10000);
			buy_PO_500_Before    = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_500_before"), -10000);
			buy_PO_100_Before    = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_100_before"), -10000);
			buy_PO_50_After      = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_50_after"), -10000);
			buy_PO_200_After     = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_200_after"), -10000);
			buy_PO_500_After     = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_500_after"), -10000);
			buy_PO_1000_After    = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_1000_after"), -10000);
			buy_PO_5000_After    = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_5000_after"), -10000);
			buy_PO_15000_After   = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_15000_after"), -10000);
			buy_PO_30000_After   = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_30000_after"), -10000);
			buy_PO_60000_After   = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_60000_after"), -10000);
			buy_PO_300000_After  = SpreadUtil.INSTANCE.calculateBps(trade.getPrice(), bidAskPriceMap.get("bid_300000_after"), -10000);	
		}
	
		final Toxicity toxicity = new Toxicity();
		toxicity.setOnbookDateKey(currentDateAsLong);
		toxicity.setTradeID(trade.getTradeID());
		toxicity.setTransactionTimestamp(trade.getTradedTimestamp());
		toxicity.setProductIsin(instrument.getIsin());
		toxicity.setProductSymbol(instrument.getSymbol());
		toxicity.setParticipant(memberID);
		if (posterAggressor.equals("AG")) {
			toxicity.setAG1000Before(getSellOrBuyValue(sell_AG_1000_Before, buy_AG_1000_Before));
			toxicity.setAG500Before(getSellOrBuyValue(sell_AG_500_Before, buy_AG_500_Before));
			toxicity.setAG100Before(getSellOrBuyValue(sell_AG_100_Before, buy_AG_100_Before));
			toxicity.setAG50After(getSellOrBuyValue(sell_AG_50_After, buy_AG_50_After));
			toxicity.setAG200After(getSellOrBuyValue(sell_AG_200_After, buy_AG_200_After));
			toxicity.setAG500After(getSellOrBuyValue(sell_AG_500_After, buy_AG_500_After));
			toxicity.setAG1000After(getSellOrBuyValue(sell_AG_1000_After, buy_AG_1000_After));
			toxicity.setAG5000After(getSellOrBuyValue(sell_AG_5000_After, buy_AG_5000_After));
			toxicity.setAG15000After(getSellOrBuyValue(sell_AG_15000_After, buy_AG_15000_After));
			toxicity.setAG30000After(getSellOrBuyValue(sell_AG_30000_After, buy_AG_30000_After));
			toxicity.setAG60000After(getSellOrBuyValue(sell_AG_60000_After, buy_AG_60000_After));
			toxicity.setAG300000After(getSellOrBuyValue(sell_AG_300000_After, buy_AG_300000_After));						
		} else {
			toxicity.setPO1000Before(getSellOrBuyValue(sell_PO_1000_Before, buy_PO_1000_Before));
			toxicity.setPO500Before(getSellOrBuyValue(sell_PO_500_Before, buy_PO_500_Before));
			toxicity.setPO100Before(getSellOrBuyValue(sell_PO_100_Before, buy_PO_100_Before));
			toxicity.setPO50After(getSellOrBuyValue(sell_PO_50_After, buy_PO_50_After));
			toxicity.setPO200After(getSellOrBuyValue(sell_PO_200_After, buy_PO_200_After));
			toxicity.setPO500After(getSellOrBuyValue(sell_PO_500_After, buy_PO_500_After));
			toxicity.setPO1000After(getSellOrBuyValue(sell_PO_1000_After, buy_PO_1000_After));
			toxicity.setPO5000After(getSellOrBuyValue(sell_PO_5000_After, buy_PO_5000_After));
			toxicity.setPO15000After(getSellOrBuyValue(sell_PO_15000_After, buy_PO_15000_After));
			toxicity.setPO30000After(getSellOrBuyValue(sell_PO_30000_After, buy_PO_30000_After));
			toxicity.setPO60000After(getSellOrBuyValue(sell_PO_60000_After, buy_PO_60000_After));
			toxicity.setPO300000After(getSellOrBuyValue(sell_PO_300000_After, buy_PO_300000_After));
		}
		return toxicity;
	}
	
	private Map<String, Double> findPrices(final Calendar cal, final TradeFact trade, TreeMap<Long, List<DailyEntireSpread>> spreadMultiMap) {
		final Map<String, Double> bidAskPriceMap = new HashMap<>();	
		bidAskPriceMap.put("bid_1000_before", SpreadUtil.INSTANCE.getBpsBestBidCeilingPrice(cal, trade, spreadMultiMap, -1000, -10000));
		bidAskPriceMap.put("bid_500_before", SpreadUtil.INSTANCE.getBpsBestBidCeilingPrice(cal, trade, spreadMultiMap, -500, -1000));
		bidAskPriceMap.put("bid_100_before", SpreadUtil.INSTANCE.getBpsBestBidCeilingPrice(cal, trade, spreadMultiMap, -100, -500));					
		bidAskPriceMap.put("bid_50_after", SpreadUtil.INSTANCE.getBpsBestBidFloorPrice(cal, trade, spreadMultiMap, 50, 200));
		bidAskPriceMap.put("bid_200_after", SpreadUtil.INSTANCE.getBpsBestBidFloorPrice(cal, trade, spreadMultiMap, 200, 500));
		bidAskPriceMap.put("bid_500_after", SpreadUtil.INSTANCE.getBpsBestBidFloorPrice(cal, trade, spreadMultiMap, 500, 1000));
		bidAskPriceMap.put("bid_1000_after", SpreadUtil.INSTANCE.getBpsBestBidFloorPrice(cal, trade, spreadMultiMap, 1000, 5000));
		bidAskPriceMap.put("bid_5000_after", SpreadUtil.INSTANCE.getBpsBestBidFloorPrice(cal, trade, spreadMultiMap, 5000, 15000));
		bidAskPriceMap.put("bid_15000_after", SpreadUtil.INSTANCE.getBpsBestBidFloorPrice(cal, trade, spreadMultiMap, 15000, 30000));
		bidAskPriceMap.put("bid_30000_after", SpreadUtil.INSTANCE.getBpsBestBidFloorPrice(cal, trade, spreadMultiMap, 30000, 60000));
		bidAskPriceMap.put("bid_60000_after", SpreadUtil.INSTANCE.getBpsBestBidFloorPrice(cal, trade, spreadMultiMap, 60000, 300000));
		bidAskPriceMap.put("bid_300000_after", SpreadUtil.INSTANCE.getBpsBestBidFloorPrice(cal, trade, spreadMultiMap, 300000, 600000));					
		bidAskPriceMap.put("ask_1000_before", SpreadUtil.INSTANCE.getBpsBestAskCeilingPrice(cal, trade, spreadMultiMap, -1000, -10000));
		bidAskPriceMap.put("ask_500_before", SpreadUtil.INSTANCE.getBpsBestAskCeilingPrice(cal, trade, spreadMultiMap, -500, -1000));
		bidAskPriceMap.put("ask_100_before", SpreadUtil.INSTANCE.getBpsBestAskCeilingPrice(cal, trade, spreadMultiMap, -100, -500));					
		bidAskPriceMap.put("ask_50_after", SpreadUtil.INSTANCE.getBpsBestAskFloorPrice(cal, trade, spreadMultiMap, 50, 200));
		bidAskPriceMap.put("ask_200_after", SpreadUtil.INSTANCE.getBpsBestAskFloorPrice(cal, trade, spreadMultiMap, 200, 500));
		bidAskPriceMap.put("ask_500_after", SpreadUtil.INSTANCE.getBpsBestAskFloorPrice(cal, trade, spreadMultiMap, 500, 1000));
		bidAskPriceMap.put("ask_1000_after", SpreadUtil.INSTANCE.getBpsBestAskFloorPrice(cal, trade, spreadMultiMap, 1000, 5000));
		bidAskPriceMap.put("ask_5000_after", SpreadUtil.INSTANCE.getBpsBestAskFloorPrice(cal, trade, spreadMultiMap, 5000, 15000));
		bidAskPriceMap.put("ask_15000_after", SpreadUtil.INSTANCE.getBpsBestAskFloorPrice(cal, trade, spreadMultiMap, 15000, 30000));
		bidAskPriceMap.put("ask_30000_after", SpreadUtil.INSTANCE.getBpsBestAskFloorPrice(cal, trade, spreadMultiMap, 30000, 60000));
		bidAskPriceMap.put("ask_60000_after", SpreadUtil.INSTANCE.getBpsBestAskFloorPrice(cal, trade, spreadMultiMap, 60000, 300000));
		bidAskPriceMap.put("ask_300000_after", SpreadUtil.INSTANCE.getBpsBestAskFloorPrice(cal, trade, spreadMultiMap, 300000, 600000));
		return bidAskPriceMap;
	}
	
	private Double getSellOrBuyValue(final Double sellValue, final Double buyValue) {
		if (sellValue != null) {
			return sellValue;
		} else {
			return buyValue;
		}
	}
	
}
