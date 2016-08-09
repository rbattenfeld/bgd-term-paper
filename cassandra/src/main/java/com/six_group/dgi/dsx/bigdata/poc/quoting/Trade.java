package com.six_group.dgi.dsx.bigdata.poc.quoting;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.six_group.dgi.dsx.bigdata.poc.parsing.ExtractValueUtil;

@Table(keyspace = "swx", name = "trade",
	readConsistency = "QUORUM",
	writeConsistency = "QUORUM",
	caseSensitiveKeyspace = false,
	caseSensitiveTable = false)
public class Trade implements Serializable {
	private static final long serialVersionUID = -3338252723406692293L;
	
	@PartitionKey(0)
	@Column(name = "security")
	private String security;
	
	@PartitionKey(1)
	@Column(name = "origdate")
	private String origdate;

	@ClusteringColumn(0)
	@Column(name = "origtime")
	private Date origtime;
	
	@ClusteringColumn(1)
	@Column(name = "id")
	private long id;
	
	@Column(name = "bidid")
	private long bidid;

	@Column(name = "askid")
	private long askid;

	@Column(name = "askliquidityind")
	private String askliquidityind;

	@Column(name = "bidliquidityind")
	private String bidliquidityind;

	@Column(name = "askmemberorg")	
	private String askmemberorg;

	@Column(name = "bidmemberorg")
	private String bidmemberorg;

	@Column(name = "askhouse")	
	private String askhouse;

	@Column(name = "bidhouse")
	private String bidhouse;

	@Column(name = "price")	
	private BigDecimal price;

	@Column(name = "volume")
	private long volume;
		
	public Trade() {
		super();
	}
	
	public Trade(String security, long id, long bidId, long askId, String askLiquidityInd, String bidLiquidityInd,
			String askMemberOrg, String bidMemberOrg, String askHouse, String bidHosue, String origdate,
			Date origtime, BigDecimal price, long volume) {
		super();
		this.security = security;
		this.id = id;
		this.bidid = bidId;
		this.askid = askId;
		this.askliquidityind = askLiquidityInd;
		this.bidliquidityind = bidLiquidityInd;
		this.askmemberorg = askMemberOrg;
		this.bidmemberorg = bidMemberOrg;
		this.askhouse = askHouse;
		this.bidhouse = bidHosue;
		this.origdate = origdate;
		this.origtime = origtime;
		this.price = price;
		this.volume = volume;
	}
	
	public static Trade getTrade(final String line, final Date origDate, final int hours, final SimpleDateFormat timeFormatter) throws ParseException {
		final Calendar cal = Calendar.getInstance();
		final Calendar calOrigDate = Calendar.getInstance();
		final Date origtime = timeFormatter.parse("01.01.2016  " + ExtractValueUtil.INSTANCE.getTime(line));
		final Trade trade = new Trade();
		trade.setAskhouse("");
		trade.setAskid(Long.valueOf(ExtractValueUtil.INSTANCE.getValue(line, " askid", true)));
		trade.setAskliquidityind(ExtractValueUtil.INSTANCE.getValue(line, "askLiquidity Ind", true));
		trade.setAskmemberorg(ExtractValueUtil.INSTANCE.getValue(line, "askMember Organisation Code", true));
		trade.setBidhouse("");
		trade.setBidid(Long.valueOf(ExtractValueUtil.INSTANCE.getValue(line, " bidid", true)));
		trade.setBidliquidityind(ExtractValueUtil.INSTANCE.getValue(line, "bidLiquidity Ind", true));
		trade.setBidmemberorg(ExtractValueUtil.INSTANCE.getValue(line, "bidMember Organisation Code", true));
		trade.setId(Long.valueOf(ExtractValueUtil.INSTANCE.getValue(line, " id", true)));
//		trade.setOrigdate(origDate);
		trade.setPrice(new BigDecimal(ExtractValueUtil.INSTANCE.getValue(line, "price", true)));
		trade.setSecurity(ExtractValueUtil.INSTANCE.getValue(line, "security", true));
		trade.setVolume(Long.valueOf(ExtractValueUtil.INSTANCE.getValue(line, "volume", true)));
		calOrigDate.setTime(origDate);
		cal.setTime(origtime);
		cal.set(Calendar.YEAR, calOrigDate.get(Calendar.YEAR));
		cal.set(Calendar.MONTH, calOrigDate.get(Calendar.MONTH));
		cal.set(Calendar.DAY_OF_MONTH, calOrigDate.get(Calendar.DAY_OF_MONTH));
		cal.add(Calendar.HOUR, hours);
		trade.setOrigtime(cal.getTime());
		return trade;
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
	
	public long getBidid() {
		return this.bidid;
	}
	
	public void setBidid(long bidId) {
		this.bidid = bidId;
	}
	
	public long getAskid() {
		return this.askid;
	}
	
	public void setAskid(long askId) {
		this.askid = askId;
	}
	
	public String getAskliquidityind() {
		return this.askliquidityind;
	}
	
	public void setAskliquidityind(String askLiquidityInd) {
		this.askliquidityind = askLiquidityInd;
	}
	
	public String getBidliquidityind() {
		return this.bidliquidityind;
	}
	
	public void setBidliquidityind(String bidLiquidityInd) {
		this.bidliquidityind = bidLiquidityInd;
	}
	
	public String getAskmemberorg() {
		return this.askmemberorg;
	}
	
	public void setAskmemberorg(String askMemberOrg) {
		this.askmemberorg = askMemberOrg;
	}
	
	public String getBidmemberorg() {
		return this.bidmemberorg;
	}
	
	public void setBidmemberorg(String bidMemberOrg) {
		this.bidmemberorg = bidMemberOrg;
	}
	
	public String getOrigdate() {
		return this.origdate;
	}
	
	public void setOrigdate(String origdate) {
		this.origdate = origdate;
	}
	
	public Date getOrigtime() {
		return this.origtime;
	}
	
	public void setOrigtime(Date origtime) {
		this.origtime = origtime;
	}
	
	public BigDecimal getPrice() {
		return this.price;
	}
	
	public void setPrice(BigDecimal price) {
		this.price = price;
	}
	
	public long getVolume() {
		return this.volume;
	}
	
	public void setVolume(long volume) {
		this.volume = volume;
	}
	
	public String getAskhouse() {
		return this.askhouse;
	}
	
	public void setAskhouse(String askHouse) {
		this.askhouse = askHouse;
	}
	
	public String getBidhouse() {
		return this.bidhouse;
	}
	
	public void setBidhouse(String bidHosue) {
		this.bidhouse = bidHosue;
	}

    @Override
    public String toString() {
        return "Trade [security=" + security + ", origdate=" + origdate + ", origtime=" + origtime + ", id=" + id
                + ", bidid=" + bidid + ", askid=" + askid + ", askliquidityind=" + askliquidityind
                + ", bidliquidityind=" + bidliquidityind + ", askmemberorg=" + askmemberorg + ", bidmemberorg="
                + bidmemberorg + ", askhouse=" + askhouse + ", bidhouse=" + bidhouse + ", price=" + price + ", volume="
                + volume + "]";
    }

}
