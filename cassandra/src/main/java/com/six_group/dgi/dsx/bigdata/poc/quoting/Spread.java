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
		private static final String _ONE_SIDED = "O";
        private static final String _TWO_SIDED = "T";
		private String house;
		private BigDecimal spread;
		private BigDecimal ask;
		private BigDecimal bid;
		private BigDecimal bestask;
		private BigDecimal bestbid;
		private Date deletetime;
        private boolean trade;
        private String cur;
        private String type = _ONE_SIDED;
        private BigDecimal price;
        private long volume;

		@PartitionKey(0)
		private String security;

		@PartitionKey(1)
		private String origdate;
						
		@ClusteringColumn(0)
		private Date origtime;
		
		@ClusteringColumn(1)
		private long id;
		
		public Spread() {	
		}
		
		public Spread(final String security, final String house, final long id, final Date origtime, final BigDecimal ask, final BigDecimal bid, final BigDecimal spread, final BigDecimal bestask, final BigDecimal bestbid, final Date deletetime, final String cur, final String type) {
            super();
            this.security = security;
			this.house = house;
			this.id = id;
			this.origtime = origtime;
			this.spread = spread;
			this.bestask = bestask;
			this.bestbid = bestbid;
			this.ask = ask;
			this.bid = bid;
			this.deletetime = deletetime;
			this.cur = cur;
			this.type = type;
			this.trade = false;
			this.price = BigDecimal.ZERO;
			this.volume = 0L;
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
            this.deletetime = null;
            this.cur = "CHF";
            this.type = _TWO_SIDED;
            this.trade = false;
            this.price = BigDecimal.ZERO;
            this.volume = 0L;
		}
		

        public Spread(final QuoteLeg leg) {
            super();
            this.security = leg.getSecurity();
            this.id = leg.getId();
            this.house = leg.getHouse();
            this.origtime = leg.getOrigtime();
            this.cur = "CHF";
            this.type = _ONE_SIDED;
            this.trade = false;
            this.price = BigDecimal.ZERO;
            this.volume = 0L;
            if (QuoteLeg.ASK.equals(leg.getSide())) {
                this.bestask = leg.getBestAsk();
                this.ask = leg.getPrice();
            } else {
                this.bestbid = leg.getBestAsk();
                this.bid = leg.getPrice();
            }
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

		public String getOrigdate() {
			return origdate;
		}

		public void setOrigdate(String origdate) {
			this.origdate = origdate;
		}

        public Date getDeletetime() {
            return deletetime;
        }

        public void setDeletetime(Date deletetime) {
            this.deletetime = deletetime;
        }

        public String getCur() {
            return cur;
        }

        public void setCur(String cur) {
            this.cur = cur;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public boolean isTrade() {
            return trade;
        }

        public void setTrade(boolean trade) {
            this.trade = trade;
        }

        public BigDecimal getPrice() {
            return price;
        }

        public void setPrice(BigDecimal price) {
            this.price = price;
        }

        public long getVolume() {
            return volume;
        }

        public void setVolume(long volume) {
            this.volume = volume;
        }

        @Override
        public String toString() {
            return "Spread [house=" + house + ", spread=" + spread + ", ask=" + ask + ", bid=" + bid + ", bestask="
                    + bestask + ", bestbid=" + bestbid + ", deletetime=" + deletetime + ", trade=" + trade + ", cur="
                    + cur + ", type=" + type + ", price=" + price + ", volume=" + volume + ", security=" + security
                    + ", origdate=" + origdate + ", origtime=" + origtime + ", id=" + id + "]";
        }
        
 }
