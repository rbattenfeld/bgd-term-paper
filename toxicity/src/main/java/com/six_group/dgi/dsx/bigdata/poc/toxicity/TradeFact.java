package com.six_group.dgi.dsx.bigdata.poc.toxicity;

import java.io.Serializable;
import java.util.Date;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "swx", name = "mrtadmin_trade_fact",
    readConsistency = "QUORUM",
    writeConsistency = "QUORUM",
    caseSensitiveKeyspace = false,
    caseSensitiveTable = false)
public class TradeFact implements Serializable {
	private static final long serialVersionUID = 7913182765371168889L;

	@PartitionKey(0)
	@Column(name = "traded_date_dim_key")
	private Long tradedDateDimKey;

	@ClusteringColumn(0)
	@Column(name = "instrument_dim_key")
	private Long instrumentDimKey;
	
	@ClusteringColumn(1)
	@Column(name = "trade_id")
	private String tradeID;
		
	@Column(name = "trading_venue_dim_key")
	private Long tradingVenueDimKey;
	
	@Column(name = "trading_environment_dim_key")
	private Long tradingEnvironmentDimKey;

	@Column(name = "buy_trading_behaviour_dim_key")
	private Long buyTradingBehaviourDimKey;
	
	@Column(name = "sell_trading_behaviour_dim_key")
	private Long sellTradingBehaviourDimKey;
	
	@Column(name = "buy_participant_dim_key")
	private Long buyParticipantDimKey;
	
	@Column(name = "sell_participant_dim_key")
	private Long sellParticipantDimKey;
	
	@Column(name = "price")
	private Double price;

	@Column(name = "volume")
	private Long volume;

	@Column(name = "traded_timestamp")
	private Date tradedTimestamp;
	
	public Long getInstrumentDimKey() {
		return instrumentDimKey;
	}

	public void setInstrumentDimKey(Long instrumentDimKey) {
		this.instrumentDimKey = instrumentDimKey;
	}

	public Long getTradingVenueDimKey() {
		return tradingVenueDimKey;
	}

	public void setTradingVenueDimKey(Long trading_venue_dim_key) {
		this.tradingVenueDimKey = trading_venue_dim_key;
	}

	public Long getTradingEnvironmentDimKey() {
		return tradingEnvironmentDimKey;
	}

	public void setTradingEnvironmentDimKey(Long trading_environment_dim_key) {
		this.tradingEnvironmentDimKey = trading_environment_dim_key;
	}

	public Long getBuyTradingBehaviourDimKey() {
		return buyTradingBehaviourDimKey;
	}

	public void setBuyTradingBehaviourDimKey(Long buy_trading_behaviour_dim_key) {
		this.buyTradingBehaviourDimKey = buy_trading_behaviour_dim_key;
	}

	public Long getSellTradingBehaviourDimKey() {
		return sellTradingBehaviourDimKey;
	}

	public void setSellTradingBehaviourDimKey(Long sell_trading_behaviour_dim_key) {
		this.sellTradingBehaviourDimKey = sell_trading_behaviour_dim_key;
	}

	public Long getBuyParticipantDimKey() {
		return buyParticipantDimKey;
	}

	public void setBuyParticipantDimKey(Long buy_participant_dim_key) {
		this.buyParticipantDimKey = buy_participant_dim_key;
	}

	public Long getSellParticipantDimKey() {
		return sellParticipantDimKey;
	}

	public void setSellParticipantDimKey(Long sell_participant_dim_key) {
		this.sellParticipantDimKey = sell_participant_dim_key;
	}

	public Double getPrice() {
		return price;
	}

	public void setPrice(Double price) {
		this.price = price;
	}

	public Long getVolume() {
		return volume;
	}

	public void setVolume(Long volume) {
		this.volume = volume;
	}

	public Date getTradedTimestamp() {
		return tradedTimestamp;
	}

	public void setTradedTimestamp(Date traded_timestamp) {
		this.tradedTimestamp = traded_timestamp;
	}

	public String getTradeID() {
		return tradeID;
	}

	public void setTradeID(String trade_id) {
		this.tradeID = trade_id;
	}

	public Long getTradedDateDimKey() {
		return tradedDateDimKey;
	}

	public void setTradedDateDimKey(Long traded_date_dim_key) {
		this.tradedDateDimKey = traded_date_dim_key;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("TradeFact [tradedDateDimKey=");
		builder.append(tradedDateDimKey);
		builder.append(", tradeID=");
		builder.append(tradeID);
		builder.append(", instrumentDimKey=");
		builder.append(instrumentDimKey);
		builder.append(", tradingVenueDimKey=");
		builder.append(tradingVenueDimKey);
		builder.append(", tradingEnvironmentDimKey=");
		builder.append(tradingEnvironmentDimKey);
		builder.append(", buyTradingBehaviourDimKey=");
		builder.append(buyTradingBehaviourDimKey);
		builder.append(", sellTradingBehaviourDimKey=");
		builder.append(sellTradingBehaviourDimKey);
		builder.append(", buyParticipantDimKey=");
		builder.append(buyParticipantDimKey);
		builder.append(", sellParticipantDimKey=");
		builder.append(sellParticipantDimKey);
		builder.append(", price=");
		builder.append(price);
		builder.append(", volume=");
		builder.append(volume);
		builder.append(", tradedTimestamp=");
		builder.append(tradedTimestamp);
		builder.append("]");
		return builder.toString();
	}
	
}
