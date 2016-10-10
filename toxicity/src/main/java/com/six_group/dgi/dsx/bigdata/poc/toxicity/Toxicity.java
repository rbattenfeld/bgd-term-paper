package com.six_group.dgi.dsx.bigdata.poc.toxicity;

import java.io.Serializable;
import java.util.Date;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "swx", name = "toxicity",
    readConsistency = "QUORUM",
    writeConsistency = "QUORUM",
    caseSensitiveKeyspace = false,
    caseSensitiveTable = false)
public class Toxicity implements Serializable {
	private static final long serialVersionUID = 7913182765371168889L;
			
	@PartitionKey(0)
	@Column(name = "onbook_date_key")
	private Long onbookDateKey;
	
	@PartitionKey(1)
	@Column(name = "participant_abbrev")
	private String participantAbbrev;
		
	@ClusteringColumn(0)
	@Column(name = "product_isin")
	private String productIsin;
	
	@ClusteringColumn(1)
	@Column(name = "trad_seg")
	private String tradingSegList;
	
	@ClusteringColumn(2)
	@Column(name = "transaction_timestamp")
    private Date transactionTimestamp;

	@ClusteringColumn(3)
	@Column(name = "trade_id")
	private String tradeID;
	
	@Column(name = "product_symbol")
	private String productSymbol;

	@Column(name = "participant")
	private Long participant;
	
	@Column(name = "AG_1000_before")
    private Double AG1000Before;

	@Column(name = "AG_500_before")
    private Double AG500Before;

	@Column(name = "AG_100_before")
    private Double AG100Before;

	@Column(name = "AG_50_after")
    private Double AG50After;

	@Column(name = "AG_200_after")
    private Double AG200After;

	@Column(name = "AG_500_after")
    private Double AG500After;

	@Column(name = "AG_1000_after")
    private Double AG1000After;

	@Column(name = "AG_5000_after")
    private Double AG5000After;

	@Column(name = "AG_15000_after")
    private Double AG15000After;

	@Column(name = "AG_30000_after")
    private Double AG30000After;

	@Column(name = "AG_60000_after")
    private Double AG60000After;

	@Column(name = "AG_300000_after")
    private Double AG300000After;

	@Column(name = "PO_1000_before")
    private Double PO1000Before;

	@Column(name = "PO_500_before")
    private Double PO500Before;

	@Column(name = "PO_100_before")
    private Double PO100Before;

	@Column(name = "PO_50_after")
    private Double PO50After;

	@Column(name = "PO_200_after")
    private Double PO200After;

	@Column(name = "PO_500_after")
    private Double PO500After;

	@Column(name = "PO_1000_after")
    private Double PO1000After;

	@Column(name = "PO_5000_after")
    private Double PO5000After;

	@Column(name = "PO_15000_after")
    private Double PO15000After;

	@Column(name = "PO_30000_after")
    private Double PO30000After;

	@Column(name = "PO_60000_after")
    private Double PO60000After;

	@Column(name = "PO_300000_after")
    private Double PO300000After;

	public String getParticipantAbbrev() {
		return participantAbbrev;
	}

	public void setParticipantAbbrev(String participantAbbrev) {
		this.participantAbbrev = participantAbbrev;
	}

	public String getTradingSegList() {
		return tradingSegList;
	}

	public void setTradingSegList(String tradingSegList) {
		this.tradingSegList = tradingSegList;
	}

	public String getTradeID() {
		return tradeID;
	}

	public void setTradeID(String tradeID) {
		this.tradeID = tradeID;
	}

	public Long getParticipant() {
		return participant;
	}

	public void setParticipant(Long participant) {
		this.participant = participant;
	}

	public Long getOnbookDateKey() {
		return onbookDateKey;
	}

	public void setOnbookDateKey(Long onbookDateKey) {
		this.onbookDateKey = onbookDateKey;
	}

	public String getProductIsin() {
		return productIsin;
	}

	public void setProductIsin(String productIsin) {
		this.productIsin = productIsin;
	}

	public Date getTransactionTimestamp() {
		return transactionTimestamp;
	}

	public void setTransactionTimestamp(Date transactionTimestamp) {
		this.transactionTimestamp = transactionTimestamp;
	}

	public String getProductSymbol() {
		return productSymbol;
	}

	public void setProductSymbol(String productSymbol) {
		this.productSymbol = productSymbol;
	}

	public Double getAG1000Before() {
		return AG1000Before;
	}

	public void setAG1000Before(Double aG1000Before) {
		AG1000Before = aG1000Before;
	}

	public Double getAG500Before() {
		return AG500Before;
	}

	public void setAG500Before(Double aG500Before) {
		AG500Before = aG500Before;
	}

	public Double getAG100Before() {
		return AG100Before;
	}

	public void setAG100Before(Double aG100Before) {
		AG100Before = aG100Before;
	}

	public Double getAG50After() {
		return AG50After;
	}

	public void setAG50After(Double aG50After) {
		AG50After = aG50After;
	}

	public Double getAG200After() {
		return AG200After;
	}

	public void setAG200After(Double aG200After) {
		AG200After = aG200After;
	}

	public Double getAG500After() {
		return AG500After;
	}

	public void setAG500After(Double aG500After) {
		AG500After = aG500After;
	}

	public Double getAG1000After() {
		return AG1000After;
	}

	public void setAG1000After(Double aG1000After) {
		AG1000After = aG1000After;
	}

	public Double getAG5000After() {
		return AG5000After;
	}

	public void setAG5000After(Double aG5000After) {
		AG5000After = aG5000After;
	}

	public Double getAG15000After() {
		return AG15000After;
	}

	public void setAG15000After(Double aG15000After) {
		AG15000After = aG15000After;
	}

	public Double getAG30000After() {
		return AG30000After;
	}

	public void setAG30000After(Double aG30000After) {
		AG30000After = aG30000After;
	}

	public Double getAG60000After() {
		return AG60000After;
	}

	public void setAG60000After(Double aG60000After) {
		AG60000After = aG60000After;
	}

	public Double getAG300000After() {
		return AG300000After;
	}

	public void setAG300000After(Double aG300000After) {
		AG300000After = aG300000After;
	}

	public Double getPO1000Before() {
		return PO1000Before;
	}

	public void setPO1000Before(Double pO1000Before) {
		PO1000Before = pO1000Before;
	}

	public Double getPO500Before() {
		return PO500Before;
	}

	public void setPO500Before(Double pO500Before) {
		PO500Before = pO500Before;
	}

	public Double getPO100Before() {
		return PO100Before;
	}

	public void setPO100Before(Double pO100Before) {
		PO100Before = pO100Before;
	}

	public Double getPO50After() {
		return PO50After;
	}

	public void setPO50After(Double pO50After) {
		PO50After = pO50After;
	}

	public Double getPO200After() {
		return PO200After;
	}

	public void setPO200After(Double pO200After) {
		PO200After = pO200After;
	}

	public Double getPO500After() {
		return PO500After;
	}

	public void setPO500After(Double pO500After) {
		PO500After = pO500After;
	}

	public Double getPO1000After() {
		return PO1000After;
	}

	public void setPO1000After(Double pO1000After) {
		PO1000After = pO1000After;
	}

	public Double getPO5000After() {
		return PO5000After;
	}

	public void setPO5000After(Double pO5000After) {
		PO5000After = pO5000After;
	}

	public Double getPO15000After() {
		return PO15000After;
	}

	public void setPO15000After(Double pO15000After) {
		PO15000After = pO15000After;
	}

	public Double getPO30000After() {
		return PO30000After;
	}

	public void setPO30000After(Double pO30000After) {
		PO30000After = pO30000After;
	}

	public Double getPO60000After() {
		return PO60000After;
	}

	public void setPO60000After(Double pO60000After) {
		PO60000After = pO60000After;
	}

	public Double getPO300000After() {
		return PO300000After;
	}

	public void setPO300000After(Double pO300000After) {
		PO300000After = pO300000After;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Toxicity [onbookDateKey=");
		builder.append(onbookDateKey);
		builder.append(", productIsin=");
		builder.append(productIsin);
		builder.append(", transactionTimestamp=");
		builder.append(transactionTimestamp);
		builder.append(", productSymbol=");
		builder.append(productSymbol);
		builder.append(", AG1000Before=");
		builder.append(AG1000Before);
		builder.append(", AG500Before=");
		builder.append(AG500Before);
		builder.append(", AG100Before=");
		builder.append(AG100Before);
		builder.append(", AG50After=");
		builder.append(AG50After);
		builder.append(", AG200After=");
		builder.append(AG200After);
		builder.append(", AG500After=");
		builder.append(AG500After);
		builder.append(", AG1000After=");
		builder.append(AG1000After);
		builder.append(", AG5000After=");
		builder.append(AG5000After);
		builder.append(", AG15000After=");
		builder.append(AG15000After);
		builder.append(", AG30000After=");
		builder.append(AG30000After);
		builder.append(", AG60000After=");
		builder.append(AG60000After);
		builder.append(", AG300000After=");
		builder.append(AG300000After);
		builder.append(", PO1000Before=");
		builder.append(PO1000Before);
		builder.append(", PO500Before=");
		builder.append(PO500Before);
		builder.append(", PO100Before=");
		builder.append(PO100Before);
		builder.append(", PO50After=");
		builder.append(PO50After);
		builder.append(", PO200After=");
		builder.append(PO200After);
		builder.append(", PO500After=");
		builder.append(PO500After);
		builder.append(", PO1000After=");
		builder.append(PO1000After);
		builder.append(", PO5000After=");
		builder.append(PO5000After);
		builder.append(", PO15000After=");
		builder.append(PO15000After);
		builder.append(", PO30000After=");
		builder.append(PO30000After);
		builder.append(", PO60000After=");
		builder.append(PO60000After);
		builder.append(", PO300000After=");
		builder.append(PO300000After);
		builder.append("]");
		return builder.toString();
	}
    
	
}
