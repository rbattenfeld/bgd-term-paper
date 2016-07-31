package com.six_group.dgi.dsx.bigdata.poc.quoting;

import java.math.BigDecimal;

public class BestBidAskSecurityCalculator {
	private final String _security;
	private BigDecimal _bestAskPrice = null;
	private BigDecimal _bestBidPrice = null;

	public BestBidAskSecurityCalculator(final String security) {
		_security = security;
	}
	
	public BigDecimal getBestAskPrice() {
		return _bestAskPrice;
	}

	public BigDecimal getBestBidPrice() {
		return _bestBidPrice;
	}

	public String getSecurity() {
		return _security;
	}

	public BigDecimal getSpread() {
		if (isCalculable()) {
			return _bestAskPrice.subtract(_bestBidPrice);
		} else {
			throw new RuntimeException("ask or bid price not set");
		}
	}

	public boolean isCalculable() {
		return _bestAskPrice != null && _bestBidPrice != null;
	}

	public void analyze(final QuoteLeg quoteLeg) {
		if (quoteLeg.getSide().equals(QuoteLeg.ASK)) {
			if (_bestAskPrice != null) {
				if (_bestAskPrice.compareTo(quoteLeg.getPrice()) > 0) {
					_bestAskPrice = quoteLeg.getPrice();
				}
			} else {
				_bestAskPrice = quoteLeg.getPrice();
			}
		} else {
			if (_bestBidPrice != null) {
				if (_bestBidPrice.compareTo(quoteLeg.getPrice()) < 0) {
					_bestBidPrice = quoteLeg.getPrice();
				}
			} else {
				_bestBidPrice = quoteLeg.getPrice();
			}
		}
	}
}
