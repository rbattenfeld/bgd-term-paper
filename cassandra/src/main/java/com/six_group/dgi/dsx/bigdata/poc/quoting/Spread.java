package com.six_group.dgi.dsx.bigdata.poc.quoting;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "swx", name = "spread",
	readConsistency = "QUORUM",
	writeConsistency = "QUORUM",
	caseSensitiveKeyspace = false,
	caseSensitiveTable = false)
public class Spread implements Serializable {
		private static final long serialVersionUID = -3338252723406692293L;
		private String house;
		private BigDecimal spread;
		private BigDecimal ask;
		private BigDecimal bid;
		private BigDecimal bestask;
		private BigDecimal bestbid;

		@PartitionKey(0)
		private String security;

		@PartitionKey(1)
		private Date origdate;
						
		@ClusteringColumn(0)
		private Date origtime;
		
		@ClusteringColumn(1)
		private long id;
		
		public Spread() {	
		}
		
		public Spread(final String security, final String house, final long id, final Date origtime, final BigDecimal ask, final BigDecimal bid, final BigDecimal spread, final BigDecimal bestask, final BigDecimal bestbid) {
			this.security = security;
			this.house = house;
			this.id = id;
			this.origtime = origtime;
			this.spread = spread;
			this.bestask = bestask;
			this.bestbid = bestbid;
			this.ask = ask;
			this.bid = bid;
		}

		public Spread(final QuoteLeg ask, final QuoteLeg bid) {
			super();
			this.security = bid.getSecurity();
			this.id = bid.getId();
			this.spread = bid.getSpread();
			this.bestask = bid.getBestAsk();
			this.bestbid = bid.getBestBid();
			this.origtime = bid.getOrigtime();
			this.house = bid.getHouse();
			this.ask = ask.getPrice();
			this.bid = bid.getPrice();
		}

		public String getSecurity() {
			return this.security;
		}

		public void setSecurity(String security) {
			this.security = security;
		}

		public long getId() {
			return this.id;
		}

		public void setId(long id) {
			this.id = id;
		}

		public String getHouse() {
			return this.house;
		}

		public void setHouse(String house) {
			this.house = house;
		}

		public Date getOrigtime() {
			return this.origtime;
		}

		public void setOrigtime(Date origtime) {
			this.origtime = origtime;
		}

		public BigDecimal getSpread() {
			return this.spread;
		}

		public void setSpread(BigDecimal spread) {
			this.spread = spread;
		}

		public BigDecimal getBestask() {
			return this.bestask;
		}

		public void setBestask(BigDecimal bestAsk) {
			this.bestask = bestAsk;
		}

		public BigDecimal getBestbid() {
			return this.bestbid;
		}

		public void setBestbid(BigDecimal bestBid) {
			this.bestbid = bestBid;
		}

		public BigDecimal getAsk() {
			return this.ask;
		}

		public void setAsk(BigDecimal ask) {
			this.ask = ask;
		}

		public BigDecimal getBid() {
			return this.bid;
		}

		public void setBid(BigDecimal bid) {
			this.bid = bid;
		}

		public Date getOrigdate() {
			return origdate;
		}

		public void setOrigdate(Date origdate) {
			this.origdate = origdate;
		}
		
 }
