package com.six_group.dgi.dsx.bigdata.poc.toxicity;

import java.io.Serializable;
import java.util.Date;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "swx", name = "mrtadmin_instrument_dim",
    readConsistency = "QUORUM",
    writeConsistency = "QUORUM",
    caseSensitiveKeyspace = false,
    caseSensitiveTable = false)
public class Instrument implements Serializable {
	private static final long serialVersionUID = 7913182765371168889L;
    
    @PartitionKey(0)
	@Column(name = "instrument_dim_key")
    private Long instrumentDimKey;
    
	@ClusteringColumn(0)
	@Column(name = "effective_start_date")
    private Date effectiveStartDate;
	
	@ClusteringColumn(1)
	@Column(name = "effective_end_date")
    private Date effectiveEndDate;
	
	@Column(name = "isin")
    private String isin;

	@Column(name = "valor")
    private String valor;

	@Column(name = "symbol")
    private String symbol;

	public Long getInstrumentDimKey() {
		return instrumentDimKey;
	}

	public void setInstrumentDimKey(Long instrumentDimKey) {
		this.instrumentDimKey = instrumentDimKey;
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

	public String getIsin() {
		return isin;
	}

	public void setIsin(String isin) {
		this.isin = isin;
	}

	public String getValor() {
		return valor;
	}

	public void setValor(String valor) {
		this.valor = valor;
	}

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}
}
