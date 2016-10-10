package com.six_group.dgi.dsx.bigdata.poc.toxicity;

import java.io.Serializable;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "swx", name = "mrtadmin_trading_behaviour_dim",
    readConsistency = "QUORUM",
    writeConsistency = "QUORUM",
    caseSensitiveKeyspace = false,
    caseSensitiveTable = false)
public class TradingBehaviour implements Serializable {
	private static final long serialVersionUID = 7913182765371168889L;

    @PartitionKey
	@Column(name = "trading_behaviour_dim_key")
    private Long tradingBehaviourDimKey;

    @ClusteringColumn
	@Column(name = "liquidity_code")
    private String liquidityCode;

	public Long getTradingBehaviourDimKey() {
		return tradingBehaviourDimKey;
	}

	public void setTradingBehaviourDimKey(Long tradingBehaviourDimKey) {
		this.tradingBehaviourDimKey = tradingBehaviourDimKey;
	}

	public String getLiquidityCode() {
		return liquidityCode;
	}

	public void setLiquidityCode(String liquidityCode) {
		this.liquidityCode = liquidityCode;
	}
  
}
