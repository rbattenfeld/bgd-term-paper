package com.six_group.dgi.dsx.bigdata.poc.toxicity;

import java.io.Serializable;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "swx", name = "mrtadmin_trading_environment_dim",
    readConsistency = "QUORUM",
    writeConsistency = "QUORUM",
    caseSensitiveKeyspace = false,
    caseSensitiveTable = false)
public class Environment implements Serializable {
	private static final long serialVersionUID = 7913182765371168889L;

    @PartitionKey
	@Column(name = "trading_environment_dim_key")
    private Long tradingEnvironmentDimKey;

    @ClusteringColumn
	@Column(name = "trading_place")
    private String tradingPlace;

    @Column(name = "trading_exchange")
    private String tradingExchange;

    @Column(name = "reversal_status")
    private String reversalStatus;

	public Long getTradingEnvironmentDimKey() {
		return tradingEnvironmentDimKey;
	}

	public void setTradingEnvironmentDimKey(Long tradingEnvironmentDimKey) {
		this.tradingEnvironmentDimKey = tradingEnvironmentDimKey;
	}

	public String getTradingPlace() {
		return tradingPlace;
	}

	public void setTradingPlace(String tradingPlace) {
		this.tradingPlace = tradingPlace;
	}

	public String getTradingExchange() {
		return tradingExchange;
	}

	public void setTradingExchange(String tradingExchange) {
		this.tradingExchange = tradingExchange;
	}

	public String getReversalStatus() {
		return reversalStatus;
	}

	public void setReversalStatus(String reversalStatus) {
		this.reversalStatus = reversalStatus;
	}
  
}
