package com.six_group.dgi.dsx.bigdata.poc.toxicity;

import java.io.Serializable;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "swx", name = "mrtadmin_trading_venue_dim",
    readConsistency = "QUORUM",
    writeConsistency = "QUORUM",
    caseSensitiveKeyspace = false,
    caseSensitiveTable = false)
public class TradingVenue implements Serializable {
	private static final long serialVersionUID = 7913182765371168889L;

    @PartitionKey
	@Column(name = "trading_venue_dim_key")
    private Long tradingVenueDimKey;

    @ClusteringColumn
	@Column(name = "trading_venue_code")
    private String tradingVenueCode;

	public Long getTradingVenueDimKey() {
		return tradingVenueDimKey;
	}

	public void setTradingVenueDimKey(Long tradingVenueDimKey) {
		this.tradingVenueDimKey = tradingVenueDimKey;
	}

	public String getTradingVenueCode() {
		return tradingVenueCode;
	}

	public void setTradingVenueCode(String tradingVenueCode) {
		this.tradingVenueCode = tradingVenueCode;
	}
  
}
