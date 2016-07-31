package com.six_group.dgi.dsx.bigdata.poc;

import java.io.Serializable;
import java.util.Date;

public class Spread implements Serializable {
		private static final long serialVersionUID = -3338252723406692293L;
		private String _security;
		private long _id;
		private String _house;
		private Date _origdate;
		private Date _origtime;
		private double _spread;
		private double _ask;
		private double _bid;
		private double _bestask;
		private double _bestbid;
		
		public Spread(final String security, final String house, final long id, final Date origtime, final double ask, final double bid, final double spread, final double bestask, final double bestbid) {
			_security = security;
			_house = house;
			_id = id;
			_origtime = origtime;
			_spread = spread;
			_bestask = bestask;
			_bestbid = bestbid;
			_ask = ask;
			_bid = bid;
		}

		public Spread(final QuoteLeg ask, final QuoteLeg bid) {
			super();
			_security = bid.getSecurity();
			_id = bid.getId();
			_spread = bid.getSpread().doubleValue();
			_bestask = bid.getBestAsk().doubleValue();
			_bestbid = bid.getBestBid().doubleValue();
			_origtime = bid.getOrigtime();
			_house = bid.getHouse();
			_ask = ask.getPrice().doubleValue();
			_bid = bid.getPrice().doubleValue();
		}

		public String getSecurity() {
			return _security;
		}

		public void setSecurity(String security) {
			_security = security;
		}

		public long getId() {
			return _id;
		}

		public void setId(long id) {
			_id = id;
		}

		public String getHouse() {
			return _house;
		}

		public void setHouse(String house) {
			_house = house;
		}

		public Date getOrigtime() {
			return _origtime;
		}

		public void setOrigtime(Date origtime) {
			_origtime = origtime;
		}

		public double getSpread() {
			return _spread;
		}

		public void setSpread(double spread) {
			_spread = spread;
		}

		public double getBestask() {
			return _bestask;
		}

		public void setBestask(double bestAsk) {
			_bestask = bestAsk;
		}

		public double getBestbid() {
			return _bestbid;
		}

		public void setBestbid(double bestBid) {
			_bestbid = bestBid;
		}

		public double getAsk() {
			return _ask;
		}

		public void setAsk(double ask) {
			_ask = ask;
		}

		public double getBid() {
			return _bid;
		}

		public void setBid(double bid) {
			_bid = bid;
		}

		public Date getOrigdate() {
			return _origdate;
		}

		public void setOrigdate(Date origdate) {
			_origdate = origdate;
		}

		
 }
