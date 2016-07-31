package com.six_group.dgi.dsx.bigdata.poc;

import java.math.BigDecimal;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class QuoteBook {
	public static final BigDecimal _TWO = BigDecimal.valueOf(2);
	public static final BigDecimal _HUNDRET = BigDecimal.valueOf(100);
    private final ConcurrentMap<Long, QuoteLeg> _askMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, QuoteLeg> _bidMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> _matchingMap = new ConcurrentHashMap<>();
    private final String _security;
    private long _matchingCount = 0;
    private QuoteLeg _bestAskPriceLegCurrent = null;
    private QuoteLeg _bestBidPriceLegCurrent = null;
    

    public QuoteBook(final String security) {
        _security = security;
    }
    
    public String getSecurity() {
        return _security;
    }

    public long getAskSize() {
        return _askMap.size();
    }

    public long getBidSize() {
        return _bidMap.size();
    }
    
    public long getMatchingCount() {
    	return _matchingCount;
    }
    
    public QuoteLeg getQuote(final long id) {
    	final QuoteLeg leg = _askMap.get(id);
    	if (leg != null) {
    		return leg;
    	} else {
    		return _bidMap.get(id);
    	}
    }
    
    public String getAskHouse(final long id) {
    	final QuoteLeg leg = _askMap.get(id);
    	if (leg != null) {
    		return leg.getHouse();
    	} else {
    		return null;
    	}
    }
    
    public String getBidHouse(final long id) {
    	final QuoteLeg leg = _bidMap.get(id);
    	if (leg != null) {
    		return leg.getHouse();
    	} else {
    		return null;
    	}
    }

    public void add(final QuoteLeg leg) {
        if (leg.getSide().equals(QuoteLeg.ASK)) {
            _askMap.put(leg.getId(), leg);
            _bestAskPriceLegCurrent = getBestAskPrice();
        } else {
            _bidMap.put(leg.getId(), leg);
            _bestBidPriceLegCurrent = getBestBidPrice();
        }
        tryToMatch(leg);
    }

    public void delete(final Long id) {
        _askMap.remove(id);
        _bidMap.remove(id);
        _bestAskPriceLegCurrent = getBestAskPrice();
    }
    
    public QuoteLeg getBestAskPrice(final QuoteLeg quoteLeg) {
    	if (_bestAskPriceLegCurrent == null) {
    		_bestAskPriceLegCurrent = getBestAskPrice();
    	} else if (_bestAskPriceLegCurrent.getPrice().compareTo(quoteLeg.getPrice()) > 0) {
    		_bestAskPriceLegCurrent = quoteLeg;
    	}
    	return _bestAskPriceLegCurrent;
    }
    
    private QuoteLeg getBestBidPrice(final QuoteLeg quoteLeg) {
    	if (_bestBidPriceLegCurrent == null) {
    		_bestBidPriceLegCurrent = getBestAskPrice();
    	} else if (_bestBidPriceLegCurrent.getPrice().compareTo(quoteLeg.getPrice()) < 0) {
    		_bestBidPriceLegCurrent = quoteLeg;
    	}
    	return _bestBidPriceLegCurrent;
    }

    private QuoteLeg getBestAskPrice() {
    	QuoteLeg bestAskPriceLeg = null;
        for (final QuoteLeg leg : _askMap.values()) {
            if (bestAskPriceLeg != null) {
                if (bestAskPriceLeg.getPrice().compareTo(leg.getPrice()) > 0) {
                    bestAskPriceLeg = leg;
                }
            } else {
                bestAskPriceLeg = leg;
            }
        }
        return bestAskPriceLeg;
    }

    private QuoteLeg getBestBidPrice() {
    	QuoteLeg bestBidPriceLeg = null;
        for (final QuoteLeg leg : _bidMap.values()) {
            if (bestBidPriceLeg != null) {
                if (bestBidPriceLeg.getPrice().compareTo(leg.getPrice()) < 0) {
                    bestBidPriceLeg = leg;
                }
            } else {
                bestBidPriceLeg = leg;
            }
        }
        return bestBidPriceLeg;
    }

    private String getOtherKey(final QuoteLeg leg) {
    	if (leg.getSide().equals("bid")) {
			return "ask" + leg.getKey();
		} else {
			return "bid" + leg.getKey();
		}
    }
    
    private boolean tryToMatch(final QuoteLeg leg) {
		final String ownKey = leg.getSide() + leg.getKey();
		final String otherKey = getOtherKey(leg);
		final Long id = _matchingMap.remove(otherKey);
		boolean IsMatched = false;
		if (id != null) {
			final QuoteLeg otherLeg = findQuote(id);
			if (otherLeg != null) {
				otherLeg.setOtherSide(leg);
				leg.setOtherSide(otherLeg);
				leg.setSpread(calculateSpread(leg));
				leg.setBestAsk(_bestAskPriceLegCurrent.getPrice());
				leg.setBestBid(_bestBidPriceLegCurrent.getPrice());
				otherLeg.setSpread(leg.getSpread());
				otherLeg.setBestAsk(_bestAskPriceLegCurrent.getPrice());
				otherLeg.setBestBid(_bestBidPriceLegCurrent.getPrice());
				IsMatched = true;
				_matchingCount++;
//				if (leg.getSpread().compareTo(BigDecimal.ZERO) == 0) {
//					System.out.println("" + leg.getSide() + " " + leg.getPrice() + " " + leg.getOtherSide().getPrice());
//				}
			} else {
				_matchingMap.put(ownKey, leg.getId());
			}
		} else {
			_matchingMap.put(ownKey, leg.getId());
		}
		return IsMatched;
	}
    
    public QuoteLeg findQuote(final Long key) {
    	final QuoteLeg leg = _askMap.get(key);
    	if (leg != null) {
    		return leg;
    	} else {
    		return _bidMap.get(key);
    	}
    }
    
    public static BigDecimal calculateSpread(final QuoteLeg leg) {
    	final BigDecimal midPrice = leg.getPrice().add(leg.getOtherSide().getPrice()).divide(_TWO);
    	if (leg.getSide().equals(QuoteLeg.ASK)) {
    		return leg.getPrice().subtract(leg.getOtherSide().getPrice()).setScale(8, BigDecimal.ROUND_HALF_DOWN).divide(midPrice, BigDecimal.ROUND_HALF_DOWN);
    	} else {
    		return leg.getOtherSide().getPrice().subtract(leg.getPrice()).setScale(8, BigDecimal.ROUND_HALF_DOWN).divide(midPrice, BigDecimal.ROUND_HALF_DOWN);
    	} 
    }
}
