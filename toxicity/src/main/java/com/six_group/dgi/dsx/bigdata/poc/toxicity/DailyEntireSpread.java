package com.six_group.dgi.dsx.bigdata.poc.toxicity;

import java.io.Serializable;
import java.util.Date;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "swx", name = "daily_entire_spread",
    readConsistency = "QUORUM",
    writeConsistency = "QUORUM",
    caseSensitiveKeyspace = false,
    caseSensitiveTable = false)
public class DailyEntireSpread implements Serializable {
	private static final long serialVersionUID = 7913182765371168889L;
    private Double spread;
    
    @PartitionKey(0)
	@Column(name = "onbook_date_key")
    private Long onbookDateKey;

    @ClusteringColumn(0)
	@Column(name = "transaction_timestamp")
    private Date transactionTimestamp;

    @ClusteringColumn(1)
	@Column(name = "product_isin")
    private String productIsin;
    
	@Column(name = "last_best_bid_price")
    private Double lastBestBidPrice;

	@Column(name = "last_best_ask_price")
    private Double lastBestAskPrice;

	@Column(name = "last_best_bid_volume")
    private Long lastBestBidVolume;

	@Column(name = "last_best_ask_volume")
    private Long lastBestAskVolume;
    
	@Column(name = "product_key")
	private Long productKey;

    @Column(name = "trading_segment_id")
	private Long tradingSegmentID;
	
	public Long getProductKey() {
		return productKey;
	}
	
	public void setProductKey(Long product_key) {
		this.productKey = product_key;
	}
	
	public Long getOnbookDateKey() {
		return onbookDateKey;
	}
	
	public void setOnbookDateKey(Long onbook_date_key) {
		this.onbookDateKey = onbook_date_key;
	}
	
	public Date getTransactionTimestamp() {
		return transactionTimestamp;
	}
	
	public void setTransactionTimestamp(Date transaction_timestamp) {
		this.transactionTimestamp = transaction_timestamp;
	}
	
	public Double getSpread() {
		return spread;
	}
	
	public void setSpread(Double spread) {
		this.spread = spread;
	}

	public Double getLastBestBidPrice() {
		return lastBestBidPrice;
	}

	public void setLastBestBidPrice(Double lastBestBidPrice) {
		this.lastBestBidPrice = lastBestBidPrice;
	}

	public Double getLastBestAskPrice() {
		return lastBestAskPrice;
	}

	public void setLastBestAskPrice(Double lastBestAskPrice) {
		this.lastBestAskPrice = lastBestAskPrice;
	}

	public Long getLastBestBidVolume() {
		return lastBestBidVolume;
	}

	public void setLastBestBidVolume(Long lastBestBidVolume) {
		this.lastBestBidVolume = lastBestBidVolume;
	}

	public Long getLastBestAskVolume() {
		return lastBestAskVolume;
	}

	public void setLastBestAskVolume(Long lastBestAskVolume) {
		this.lastBestAskVolume = lastBestAskVolume;
	}

	public String getProductIsin() {
		return productIsin;
	}

	public void setProductIsin(String productIsin) {
		this.productIsin = productIsin;
	}

	public Long getTradingSegmentID() {
		return tradingSegmentID;
	}

	public void setTradingSegmentID(Long tradingSegmentID) {
		this.tradingSegmentID = tradingSegmentID;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("DailyEntireSpread [spread=");
		builder.append(spread);
		builder.append(", lastBestBidPrice=");
		builder.append(lastBestBidPrice);
		builder.append(", lastBestAskPrice=");
		builder.append(lastBestAskPrice);
		builder.append(", lastBestBidVolume=");
		builder.append(lastBestBidVolume);
		builder.append(", lastBestAskVolume=");
		builder.append(lastBestAskVolume);
		builder.append(", productKey=");
		builder.append(productKey);
		builder.append(", transactionTimestamp=");
		builder.append(transactionTimestamp);
		builder.append(", productIsin=");
		builder.append(productIsin);
		builder.append(", onbookDateKey=");
		builder.append(onbookDateKey);
		builder.append("]");
		return builder.toString();
	}

}
