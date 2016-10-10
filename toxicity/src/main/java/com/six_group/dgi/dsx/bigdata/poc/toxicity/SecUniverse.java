package com.six_group.dgi.dsx.bigdata.poc.toxicity;

import java.io.Serializable;
import java.util.Date;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "swx", name = "sec_universe",
    readConsistency = "QUORUM",
    writeConsistency = "QUORUM",
    caseSensitiveKeyspace = false,
    caseSensitiveTable = false)
public class SecUniverse implements Serializable {
	private static final long serialVersionUID = 7913182765371168889L;

	@PartitionKey(0)
	@Column(name = "product_key")
	private Long productKey;
	
	@ClusteringColumn(0)
	@Column(name = "product_symbol")
	private String productSymbol;
	
	@ClusteringColumn(1)
	@Column(name = "product_isin")
	private String productIsin;
	
	@Column(name = "trading_segment_id")
	private Long tradingSegmentID;
	
	@Column(name = "effective_start_date")
	private Date effectiveStartDate;
	
	@Column(name = "effective_end_date")
	private Date effectiveEndDate;

	public Long getProductKey() {
		return productKey;
	}

	public void setProductKey(Long productKey) {
		this.productKey = productKey;
	}

	public String getProductSymbol() {
		return productSymbol;
	}

	public void setProductSymbol(String productSymbol) {
		this.productSymbol = productSymbol;
	}

	public String getProductIsin() {
		return productIsin;
	}

	public void setProductIsin(String productIsin) {
		this.productIsin = productIsin;
	}

	public Date getEffectiveStartDate() {
		return effectiveStartDate;
	}

	public void setEffectiveStartDate(Date effectiveStartDate) {
		this.effectiveStartDate = effectiveStartDate;
	}

	public Date getEffectiveEndDate() {
		return effectiveEndDate;
	}

	public void setEffectiveEndDate(Date effectiveEndDate) {
		this.effectiveEndDate = effectiveEndDate;
	}

	public Long getTradingSegmentID() {
		return tradingSegmentID;
	}

	public void setTradingSegmentID(Long tradingSegmentID) {
		this.tradingSegmentID = tradingSegmentID;
	}
    
}
