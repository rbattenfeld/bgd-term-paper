package com.six_group.dgi.dsx.bigdata.poc;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Trade implements Serializable {
	private static final long serialVersionUID = -3338252723406692293L;
	private String _security;
	private long _id;
	private long _bidid;
	private long _askid;
	private String _askliquidityind;
	private String _bidliquidityind;	
	private String _askmemberorg;
	private String _bidmemberorg;	
	private String _askhouse;
	private String _bidhouse;	
	private Date _origdate;
	private Date _origtime;
	private double _price;
	private long _volume;
	
	public Trade() {
		super();
	}
	
	public Trade(String security, long id, long bidId, long askId, String askLiquidityInd, String bidLiquidityInd,
			String askMemberOrg, String bidMemberOrg, String askHouse, String bidHosue, Date origdate,
			Date origtime, double price, long volume) {
		super();
		_security = security;
		_id = id;
		_bidid = bidId;
		_askid = askId;
		_askliquidityind = askLiquidityInd;
		_bidliquidityind = bidLiquidityInd;
		_askmemberorg = askMemberOrg;
		_bidmemberorg = bidMemberOrg;
		_askhouse = askHouse;
		_bidhouse = bidHosue;
		_origdate = origdate;
		_origtime = origtime;
		_price = price;
		_volume = volume;
	}


	//Q1 09:59:42.407 TRADE: security="ABBN" transid=0 id=538313771634 price=22.66 volume=295 value=6684.7 flags=Bi,QM,Q `trdNostro Cross="No" `trdTPI="P" `trdTrade 
//			Sub Type="OnExchange" bidid=171310000271550 `bidTCC="-" `bidOrderType="-" `bidEntrySource="-" `bidOrderDuration="-" `bidBTT="-" `bidReq TPI="-" `bidLiquidity Ind="Aggressor" 
//			*bidMember Organisation Code="MLCGB1_281_" *bidTrade Book Type="OnBook" askid=171310000268618 `askTCC="-" `askOrderType="-" `askEntrySource="-" `askOrderDuration="-" `askBTT="-" 
//			`askReq TPI="-" 
//			`askLiquidity Ind="Poster" *askMember Organisation Code="GSIGB_303_"
	
	public static Trade getTrade(final String line, final Date origDate, final int hours, final SimpleDateFormat timeFormatter) throws ParseException {
		final Calendar cal = Calendar.getInstance();
		final Calendar calOrigDate = Calendar.getInstance();
		final Date origtime = timeFormatter.parse("01.01.2016  " + getTime(line));
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
		trade.setOrigdate(origDate);
		trade.setPrice(Double.valueOf(ExtractValueUtil.INSTANCE.getValue(line, "price", true)));
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
		return _security;
	}
	
	public void setSecurity(String security) {
		_security = security;
	}
	
	public long getId() {
		return _id;
	}
	
	public void setId(long id) {
		_id = id;
	}
	
	public long getBidid() {
		return _bidid;
	}
	
	public void setBidid(long bidId) {
		_bidid = bidId;
	}
	
	public long getAskid() {
		return _askid;
	}
	
	public void setAskid(long askId) {
		_askid = askId;
	}
	
	public String getAskliquidityind() {
		return _askliquidityind;
	}
	
	public void setAskliquidityind(String askLiquidityInd) {
		_askliquidityind = askLiquidityInd;
	}
	
	public String getBidliquidityind() {
		return _bidliquidityind;
	}
	
	public void setBidliquidityind(String bidLiquidityInd) {
		_bidliquidityind = bidLiquidityInd;
	}
	
	public String getAskmemberorg() {
		return _askmemberorg;
	}
	
	public void setAskmemberorg(String askMemberOrg) {
		_askmemberorg = askMemberOrg;
	}
	
	public String getBidmemberorg() {
		return _bidmemberorg;
	}
	
	public void setBidmemberorg(String bidMemberOrg) {
		_bidmemberorg = bidMemberOrg;
	}
	
	public Date getOrigdate() {
		return _origdate;
	}
	
	public void setOrigdate(Date origdate) {
		_origdate = origdate;
	}
	
	public Date getOrigtime() {
		return _origtime;
	}
	
	public void setOrigtime(Date origtime) {
		_origtime = origtime;
	}
	
	public double getPrice() {
		return _price;
	}
	
	public void setPrice(double price) {
		_price = price;
	}
	
	public long getVolume() {
		return _volume;
	}
	
	public void setVolume(long volume) {
		_volume = volume;
	}
	
	public String getAskhouse() {
		return _askhouse;
	}
	
	public void setAskhouse(String askHouse) {
		_askhouse = askHouse;
	}
	
	public String getBidhouse() {
		return _bidhouse;
	}
	
	public void setBidhouse(String bidHosue) {
		_bidhouse = bidHosue;
	}

	private static String getTime(final String line) {
		return line.substring(3, 15);
	}
}
