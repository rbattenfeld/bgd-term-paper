package com.six_group.dgi.dsx.bigdata.poc;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class QuoteLeg implements Serializable {
	public static final String ASK = "ask";
	public static final String BID = "bid";
	private static final long serialVersionUID = -3338252723406692293L;
	private String _security;
	private String _side;
	private long _id;
	private BigDecimal _price;
	private long _volume;
	private String _entrySource;
	private String _btt;
	private String _house;
	private String _user;
	private String _booktype;
	private Date _origtime;
	private String _memorg;
	private String _key;
	private BigDecimal _spread;
	private BigDecimal _bestAsk;
	private BigDecimal _bestBid;
	private QuoteLeg _otherSide;

	public QuoteLeg() {
	}

	public QuoteLeg(String security, String side, long id, BigDecimal price, BigDecimal bestAsk, BigDecimal bestBid, long volume, String entrySource,
			String btt, String house, String user, String booktype, Date origtime, String memorg) {
		super();
		_security = security;
		_side = side;
		_id = id;
		_price = price;
		_bestAsk = bestAsk;
		_bestBid = bestBid;
		_volume = volume;
		_entrySource = entrySource;
		_btt = btt;
		_house = house;
		_user = user;
		_booktype = booktype;
		_origtime = origtime;
		_memorg = memorg;
		_key = createKey();
		_price.setScale(8, BigDecimal.ROUND_HALF_DOWN);
		
	}

	public static QuoteLeg getLeg(final String line, final SimpleDateFormat timeFormatter) throws NumberFormatException, ParseException {
		return new QuoteLeg(
		        ExtractValueUtil.INSTANCE.getValue(line, "security", true),
		        ExtractValueUtil.INSTANCE.getValue(line, "side", true), 
		        Long.valueOf(ExtractValueUtil.INSTANCE.getValue(line, " id", true)),
				new BigDecimal(ExtractValueUtil.INSTANCE.getValue(line, "price", true)),
				BigDecimal.ZERO,
				BigDecimal.ZERO,
			    Long.valueOf(ExtractValueUtil.INSTANCE.getValue(line, "volume", true)),
			    ExtractValueUtil.INSTANCE.getValue(line, "EntrySource", false),
			    ExtractValueUtil.INSTANCE.getValue(line, "BTT", false),
			    ExtractValueUtil.INSTANCE.getValue(line, "house", false),
			    ExtractValueUtil.INSTANCE.getValue(line, "User",false),
			    ExtractValueUtil.INSTANCE.getValue(line, "Trade Book Type", false),
			    timeFormatter.parse(ExtractValueUtil.INSTANCE.getValue(line, "Original Date", true) + " " + ExtractValueUtil.INSTANCE.getValue(line, "Original Time", true)),
			    ExtractValueUtil.INSTANCE.getValue(line, "ember Organisation Code", true));
	}

	public QuoteLeg getOtherSide() {
		return _otherSide;
	}

	public void setOtherSide(QuoteLeg otherSide) {
		_otherSide = otherSide;
	}
	
	public QuoteLeg getAskLeg() {
		if (getSide().equals(QuoteLeg.ASK)) {
    		return this;
    	} else {
    		return getOtherSide();
    	} 
	}
	
	public QuoteLeg getBidLeg() {
		if (getSide().equals(QuoteLeg.BID)) {
    		return this;
    	} else {
    		return getOtherSide();
    	} 
	}
	
	private String createKey() {
		StringBuffer buf = new StringBuffer();
		buf.append(getSecurity());
		buf.append(getHouse());
		buf.append(getOrigtime());
//		buf.append(getOrigdate());
		return buf.toString();
	}

	public String getKey() {
		return _key;
	}
	
	public String getSecurity() {
		return _security;
	}

	public void setSecurity(String security) {
		_security = security;
	}

	public String getSide() {
		return _side;
	}

	public void setSide(String side) {
		_side = side;
	}

	public long getId() {
		return _id;
	}

	public void setId(long id) {
		_id = id;
	}

	public BigDecimal getPrice() {
		return _price;
	}

	public void setPrice(BigDecimal price) {
		_price = price;
		_price.setScale(8, BigDecimal.ROUND_HALF_DOWN);
	}

	public long getVolume() {
		return _volume;
	}

	public void setVolume(long volume) {
		_volume = volume;
	}

	public String getEntrySource() {
		return _entrySource;
	}

	public void setEntrySource(String entrySource) {
		_entrySource = entrySource;
	}

	public String getBtt() {
		return _btt;
	}

	public void setBtt(String btt) {
		_btt = btt;
	}

	public String getHouse() {
		return _house;
	}

	public void setHouse(String house) {
		_house = house;
	}

	public String getUser() {
		return _user;
	}

	public void setUser(String user) {
		_user = user;
	}

	public String getBookType() {
		return _booktype;
	}

	public void setBookType(String bookType) {
		_booktype = bookType;
	}

	public Date getOrigtime() {
		return _origtime;
	}

	public void setOrigtime(Date origtime) {
		_origtime = origtime;
	}

	public String getMemorg() {
		return _memorg;
	}

	public void setMemorg(String memOrg) {
		_memorg = memOrg;
	}

	public BigDecimal getSpread() {
		return _spread;
	}

	public void setSpread(BigDecimal spread) {
		_spread = spread;
	}

	public BigDecimal getBestAsk() {
		return _bestAsk;
	}

	public void setBestAsk(BigDecimal bestAsk) {
		_bestAsk = bestAsk;
	}

	public BigDecimal getBestBid() {
		return _bestBid;
	}

	public void setBestBid(BigDecimal bestBid) {
		_bestBid = bestBid;
	}

	
}
