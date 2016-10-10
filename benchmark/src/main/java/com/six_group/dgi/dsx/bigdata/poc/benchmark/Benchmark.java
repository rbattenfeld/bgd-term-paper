package com.six_group.dgi.dsx.bigdata.poc.benchmark;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "swx", name = "Benchmark",
	readConsistency = "QUORUM",
	writeConsistency = "QUORUM",
	caseSensitiveKeyspace = false,
	caseSensitiveTable = false)
public class Benchmark implements Serializable {
	private static final long serialVersionUID = -3338252723406692293L;
	
	@PartitionKey
	private String businessMonth;
	
	@ClusteringColumn(0)
	private String businessDate;
	
	@ClusteringColumn(1)
	private String isin;

	@ClusteringColumn(2)
	private String tradeID;
	
	private String fileName;
	private String ccy;
	private String fullName;
	private Date tradeTime;
	private BigDecimal tradePri;
	private Long tradeQty;
	private String tidm;
	private String side;
	private String tradingNetworkID;
	private String trader;
	private String counterparty;
	private String orderRef;
	private String participant;
	private String portfolioManagerID; 
	private String userDefined; 
	private BigDecimal tradeTiBenPriTouch;
	private BigDecimal hiLoBenPriTouch;
	private BigDecimal tradeTiBenPriEff; 
	private BigDecimal hiLoBenPriEff;
	private String tradeTiBenVenTouch;
	private String hiLoBenVenTouch;
	private String tradeTiBenVenEff;
	private String hiLoBenVenEff;
	private String tradeTiBenBPSTouch;
	private String hiLoBenBPSTouch;
	private String tradeTiBenBPsEff;
	private String hiLoBenBPSEff;
	private String tradeTiBenSfallValueTouch;
	private String hiLoBenSfallValueTouch;
	private String tradeTiBenSfallValueEff;
	private String hiLoBenSfallValueEff;
	private BigDecimal xswxBBidPri;
	private Long xswxBBidVol;
	private BigDecimal xswxBOffPri;
	private Long xswxBOffVol;
	private BigDecimal xswxEffBidPri;
	private BigDecimal xswxEffOffPri;
	private BigDecimal bateBBidPri;
	private Long bateBBidVol;
	private BigDecimal bateBOffPri;
	private Long bateBOffVol;
	private BigDecimal bateEffBidPri;
	private BigDecimal bateEffOffPri;
	private BigDecimal chixBBidPri;
	private Long chixBBidVol;
	private BigDecimal chixBOffPri;
	private Long chixBOffVol;
	private BigDecimal chixEffBidPri;
	private BigDecimal chixEffOffPri;
	private BigDecimal trqxBBidPri;
	private Long trqxBBidVol;
	private BigDecimal trqxBOffPri;
	private Long trqxBOffVol;
	private BigDecimal trqxEffBidPri;
	private BigDecimal trqxEffOffPri;
	
	private BigDecimal aqxeBBidPri;
	private Long aqxeBBidVol;
	private BigDecimal aqxeBOffPri;
	private Long aqxeBOffVol;
	private BigDecimal aqxeEffBidPri;
	private BigDecimal aqxeEffOffPri;
	
	private BigDecimal BBidPriXSWXDiffBATE;
	private BigDecimal BBidPriXSWXDiffCHIX;
	private BigDecimal BBidPriXSWXDiffTRQX;
	private BigDecimal BBidPriXSWXDiffAQXE;
	private BigDecimal spreadXswx;
	private BigDecimal spreadBate;
	private BigDecimal spreadChix;
	private BigDecimal spreadTrqx;
	private BigDecimal spreadAqxe;
	private String highBidPriVen; 
	private String highBidPriRanking; 
	private String lowOffPriRanking;
    
	public static Benchmark getBenchmark(final String fileName, final String businessDate, final String line) {
		final String[] items = line.split(",", -1);	
		if (items.length == 62) {
			final Benchmark Ben = new Benchmark();
			Ben.setBusinessMonth(businessDate.substring(0, 6));
			Ben.setBusinessDate(businessDate);
			Ben.setFileName(fileName);			
			Ben.setIsin(items[0]);
			Ben.setCcy(items[1]);
			Ben.setFullName(items[2]);
			Ben.setTradeTime(new Date(Long.valueOf(items[3])));
			Ben.setTradePri(new BigDecimal(items[4]));
			Ben.setTradeQty(setLong(items[5]));
			Ben.setTradeID(items[6]);
			Ben.setTidm(items[7]);
			Ben.setSide(items[8]);
			Ben.setTradingNetworkID(items[9]);
			Ben.setTrader(items[10]);
			Ben.setCounterparty(items[11]);
			Ben.setOrderRef(items[12]);
			Ben.setParticipant(items[13]);
			Ben.setPortfolioManagerID(items[14]); 
			Ben.setUserDefined(items[15]); 
			Ben.setTradeTiBenPriTouch(setBigDecimal(items[16]));
			Ben.setHiLoBenPriTouch(setBigDecimal(items[17]));
			Ben.setTradeTiBenPriEff(setBigDecimal(items[18])); 
			Ben.setHiLoBenPriEff(setBigDecimal(items[19]));
			Ben.setTradeTiBenVenTouch(items[20]);
			Ben.setHiLoBenVenTouch(items[21]);
			Ben.setTradeTiBenVenEff(items[22]);
			Ben.setHiLoBenVenEff(items[23]);
			Ben.setTradeTiBenBPSTouch(items[24]);
			Ben.setHiLoBenBPSTouch(items[25]);
			Ben.setTradeTiBenBPsEff(items[26]);
			Ben.setHiLoBenBPSEff(items[27]);
			Ben.setTradeTiBenSfallValueTouch(items[28]);
			Ben.setHiLoBenSfallValueTouch(items[29]);
			Ben.setTradeTiBenSfallValueEff(items[30]);
			Ben.setHiLoBenSfallValueEff(items[31]);
			Ben.setXswxBBidPri(setBigDecimal(items[32]));
			Ben.setXswxBBidVol(setLong(items[33]));
			Ben.setXswxBOffPri(setBigDecimal(items[34]));
			Ben.setXswxBOffVol(setLong(items[35]));
			Ben.setXswxEffBidPri(setBigDecimal(items[36]));
			Ben.setXswxEffOffPri(setBigDecimal(items[37]));
			Ben.setBateBBidPri(setBigDecimal(items[38]));
			Ben.setBateBBidVol(setLong(items[39]));
			Ben.setBateBOffPri(setBigDecimal(items[40]));
			Ben.setBateBOffVol(setLong(items[41]));
			Ben.setBateEffBidPri(setBigDecimal(items[42]));
			Ben.setBateEffOffPri(setBigDecimal(items[43]));
			Ben.setChixBBidPri(setBigDecimal(items[44]));
			Ben.setChixBBidVol(setLong(items[45]));
			Ben.setChixBOffPri(setBigDecimal(items[46]));
			Ben.setChixBOffVol(setLong(items[47]));
			Ben.setChixEffBidPri(setBigDecimal(items[48]));
			Ben.setChixEffOffPri(setBigDecimal(items[49]));
			Ben.setTrqxBBidPri(setBigDecimal(items[50]));
			Ben.setTrqxBBidVol(setLong(items[51]));
			Ben.setTrqxBOffPri(setBigDecimal(items[52]));
			Ben.setTrqxBOffVol(setLong(items[53]));
			Ben.setTrqxEffBidPri(setBigDecimal(items[54]));
			Ben.setTrqxEffOffPri(setBigDecimal(items[55]));
			Ben.setAqxeBBidPri(setBigDecimal(items[56]));
			Ben.setAqxeBBidVol(setLong(items[57]));
			Ben.setAqxeBOffPri(setBigDecimal(items[58]));
			Ben.setAqxeBOffVol(setLong(items[59]));
			Ben.setAqxeEffBidPri(setBigDecimal(items[60]));
			Ben.setAqxeEffOffPri(setBigDecimal(items[61]));
			
			final Map<BigDecimal, String> BBidPriMap = new HashMap<>();
			final Map<BigDecimal, String> BOffPriMap = new HashMap<>();
			ExtractValueUtil.INSTANCE.addToMap(BBidPriMap, "XSWX", Ben.getXswxBBidPri());
	    	ExtractValueUtil.INSTANCE.addToMap(BBidPriMap, "BATE", Ben.getBateBBidPri());
	    	ExtractValueUtil.INSTANCE.addToMap(BBidPriMap, "CHIX", Ben.getChixBBidPri());
	    	ExtractValueUtil.INSTANCE.addToMap(BBidPriMap, "TRQX", Ben.getTrqxBBidPri());
	    	ExtractValueUtil.INSTANCE.addToMap(BBidPriMap, "AQXE", Ben.getAqxeBBidPri());    	
			ExtractValueUtil.INSTANCE.addToMap(BOffPriMap, "XSWX", Ben.getXswxBOffPri());
	    	ExtractValueUtil.INSTANCE.addToMap(BOffPriMap, "BATE", Ben.getBateBOffPri());
	    	ExtractValueUtil.INSTANCE.addToMap(BOffPriMap, "CHIX", Ben.getChixBOffPri());
	    	ExtractValueUtil.INSTANCE.addToMap(BOffPriMap, "TRQX", Ben.getTrqxBOffPri());
	    	ExtractValueUtil.INSTANCE.addToMap(BOffPriMap, "AQXE", Ben.getAqxeBOffPri());	    	
	    	final Map<BigDecimal, String> BBidPriSortedMap = ExtractValueUtil.INSTANCE.sortMap(BBidPriMap, true);
	    	final Map<BigDecimal, String> BOffPriSortedMap = ExtractValueUtil.INSTANCE.sortMap(BOffPriMap, false);	    	
	    	Ben.sethighBidPriRanking(ExtractValueUtil.INSTANCE.getValuesAsString(BBidPriSortedMap));
	    	Ben.setlowOffPriRanking(ExtractValueUtil.INSTANCE.getValuesAsString(BOffPriSortedMap));
	
			BigDecimal BBid = BigDecimal.ZERO;			
			final Map<String, BigDecimal> BBidMap = new HashMap<>();	
			if (Ben.getXswxBBidPri() != null) {
				if (BBid.compareTo(Ben.getXswxBBidPri()) < 0) {
					BBid = Ben.getXswxBBidPri();
					Ben.sethighBidPriVen("XSWX");
				}
			}
			
			if (Ben.getBateBBidPri() != null) {
				if (BBid.compareTo(Ben.getBateBBidPri()) < 0) {
					BBid = Ben.getBateBBidPri();
					Ben.sethighBidPriVen("BATE");
				}
			}
			
			if (Ben.getChixBBidPri() != null) {
				if (BBid.compareTo(Ben.getChixBBidPri()) < 0) {
					BBid = Ben.getChixBBidPri();
					Ben.sethighBidPriVen("CHIX");
				}
			}
			
			if (Ben.getTrqxBBidPri() != null) {
				if (BBid.compareTo(Ben.getTrqxBBidPri()) < 0) {
					BBid = Ben.getTrqxBBidPri();
					Ben.sethighBidPriVen("TRQX");
				}
			}

			if (Ben.getAqxeBBidPri() != null) {
				if (BBid.compareTo(Ben.getAqxeBBidPri()) < 0) {
					BBid = Ben.getAqxeBBidPri();
					Ben.sethighBidPriVen("AQXE");
				}
			}
			
			if (Ben.getXswxBBidPri() != null) {
			    if (Ben.getBateBBidPri() != null) {
			        Ben.setBBidPriXSWXDiffBATE(Ben.getXswxBBidPri().subtract(Ben.getBateBBidPri()));
			    }
			    if (Ben.getChixBBidPri() != null) {
                    Ben.setBBidPriXSWXDiffCHIX(Ben.getXswxBBidPri().subtract(Ben.getChixBBidPri()));
                }
			    if (Ben.getTrqxBBidPri() != null) {
                    Ben.setBBidPriXSWXDiffTRQX(Ben.getXswxBBidPri().subtract(Ben.getTrqxBBidPri()));
                }
			    if (Ben.getAqxeBBidPri() != null) {
                    Ben.setBBidPriXSWXDiffAQXE(Ben.getXswxBBidPri().subtract(Ben.getAqxeBBidPri()));
                }
			}
			
			return Ben;
		}
		throw new IllegalArgumentException("Array length mismatch");
	}

	public Benchmark() {
		super();
	}	
	
	public Benchmark(String businessDate, String isin, String tradeID, String fileName, String ccy, String fullName,
			Date tradeTime, BigDecimal tradePri, Long tradeQty, String tidm, String side, String tradingNetworkID,
			String trader, String counterparty, String orderRef, String participant, String portfolioManagerID,
			String userDefined, BigDecimal tradeTiBenPriTouch, BigDecimal hiLoBenPriTouch,
			BigDecimal tradeTiBenPriEff, BigDecimal hiLoBenPriEff,
			String tradeTiBenVenTouch, String hiLoBenVenTouch,
			String tradeTiBenVenEff, String hiLoBenVenEff,
			String tradeTiBenBPSTouch, String hiLoBenBPSTouch, String tradeTiBenBPsEff,
			String hiLoBenBPSEff, String tradeTiBenSfallValueTouch,
			String hiLoBenSfallValueTouch, String tradeTiBenSfallValueEff,
			String hiLoBenSfallValueEff, BigDecimal xswxBBidPri, Long xswxBBidVol,
			BigDecimal xswxBOffPri, Long xswxBOffVol, BigDecimal xswxEffBidPri,
			BigDecimal xswxEffOffPri, BigDecimal bateBBidPri, Long bateBBidVol,
			BigDecimal bateBOffPri, Long bateBOffVol, BigDecimal bateEffBidPri,
			BigDecimal bateEffOffPri, BigDecimal chixBBidPri, Long chixBBidVol,
			BigDecimal chixBOffPri, Long chixBOffVol, BigDecimal chixEffBidPri,
			BigDecimal chixEffOffPri, BigDecimal trqxBBidPri, Long trqxBBidVol,
			BigDecimal trqxBOffPri, Long trqxBOffVol, BigDecimal trqxEffBidPri,
			BigDecimal trqxEffOffPri) {
		super();
		this.businessDate = businessDate;
		this.isin = isin;
		this.tradeID = tradeID;
		this.fileName = fileName;
		this.ccy = ccy;
		this.fullName = fullName;
		this.tradeTime = tradeTime;
		this.tradePri = tradePri;
		this.tradeQty = tradeQty;
		this.tidm = tidm;
		this.side = side;
		this.tradingNetworkID = tradingNetworkID;
		this.trader = trader;
		this.counterparty = counterparty;
		this.orderRef = orderRef;
		this.participant = participant;
		this.portfolioManagerID = portfolioManagerID;
		this.userDefined = userDefined;
		this.tradeTiBenPriTouch = tradeTiBenPriTouch;
		this.hiLoBenPriTouch = hiLoBenPriTouch;
		this.tradeTiBenPriEff = tradeTiBenPriEff;
		this.hiLoBenPriEff = hiLoBenPriEff;
		this.tradeTiBenVenTouch = tradeTiBenVenTouch;
		this.hiLoBenVenTouch = hiLoBenVenTouch;
		this.tradeTiBenVenEff = tradeTiBenVenEff;
		this.hiLoBenVenEff = hiLoBenVenEff;
		this.tradeTiBenBPSTouch = tradeTiBenBPSTouch;
		this.hiLoBenBPSTouch = hiLoBenBPSTouch;
		this.tradeTiBenBPsEff = tradeTiBenBPsEff;
		this.hiLoBenBPSEff = hiLoBenBPSEff;
		this.tradeTiBenSfallValueTouch = tradeTiBenSfallValueTouch;
		this.hiLoBenSfallValueTouch = hiLoBenSfallValueTouch;
		this.tradeTiBenSfallValueEff = tradeTiBenSfallValueEff;
		this.hiLoBenSfallValueEff = hiLoBenSfallValueEff;
		this.xswxBBidPri = xswxBBidPri;
		this.xswxBBidVol = xswxBBidVol;
		this.xswxBOffPri = xswxBOffPri;
		this.xswxBOffVol = xswxBOffVol;
		this.xswxEffBidPri = xswxEffBidPri;
		this.xswxEffOffPri = xswxEffOffPri;
		this.bateBBidPri = bateBBidPri;
		this.bateBBidVol = bateBBidVol;
		this.bateBOffPri = bateBOffPri;
		this.bateBOffVol = bateBOffVol;
		this.bateEffBidPri = bateEffBidPri;
		this.bateEffOffPri = bateEffOffPri;
		this.chixBBidPri = chixBBidPri;
		this.chixBBidVol = chixBBidVol;
		this.chixBOffPri = chixBOffPri;
		this.chixBOffVol = chixBOffVol;
		this.chixEffBidPri = chixEffBidPri;
		this.chixEffOffPri = chixEffOffPri;
		this.trqxBBidPri = trqxBBidPri;
		this.trqxBBidVol = trqxBBidVol;
		this.trqxBOffPri = trqxBOffPri;
		this.trqxBOffVol = trqxBOffVol;
		this.trqxEffBidPri = trqxEffBidPri;
		this.trqxEffOffPri = trqxEffOffPri;
	}
	
	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getBusinessDate() {
		return businessDate;
	}

	public void setBusinessDate(String businessDate) {
		this.businessDate = businessDate;
	}

	public String getIsin() {
		return isin;
	}

	public void setIsin(String isin) {
		this.isin = isin;
	}

	public String getCcy() {
		return ccy;
	}

	public void setCcy(String ccy) {
		this.ccy = ccy;
	}

	public String getFullName() {
		return fullName;
	}

	public void setFullName(String fullName) {
		this.fullName = fullName;
	}

	public Date getTradeTime() {
		return tradeTime;
	}

	public void setTradeTime(Date tradeTime) {
		this.tradeTime = tradeTime;
	}

	public BigDecimal getTradePri() {
		return tradePri;
	}

	public void setTradePri(BigDecimal tradePri) {
		this.tradePri = tradePri;
	}

	public Long getTradeQty() {
		return tradeQty;
	}

	public void setTradeQty(Long tradeQty) {
		this.tradeQty = tradeQty;
	}

	public String getTradeID() {
		return tradeID;
	}

	public void setTradeID(String tradeID) {
		this.tradeID = tradeID;
	}

	public String getTidm() {
		return tidm;
	}

	public void setTidm(String tidm) {
		this.tidm = tidm;
	}

	public String getSide() {
		return side;
	}

	public void setSide(String side) {
		this.side = side;
	}

	public String getTradingNetworkID() {
		return tradingNetworkID;
	}

	public void setTradingNetworkID(String tradingNetworkID) {
		this.tradingNetworkID = tradingNetworkID;
	}

	public String getTrader() {
		return trader;
	}

	public void setTrader(String trader) {
		this.trader = trader;
	}

	public String getCounterparty() {
		return counterparty;
	}

	public void setCounterparty(String counterparty) {
		this.counterparty = counterparty;
	}

	public String getOrderRef() {
		return orderRef;
	}

	public void setOrderRef(String orderRef) {
		this.orderRef = orderRef;
	}

	public String getParticipant() {
		return participant;
	}

	public void setParticipant(String participant) {
		this.participant = participant;
	}

	public String getPortfolioManagerID() {
		return portfolioManagerID;
	}

	public void setPortfolioManagerID(String portfolioManagerID) {
		this.portfolioManagerID = portfolioManagerID;
	}

	public String getUserDefined() {
		return userDefined;
	}

	public void setUserDefined(String userDefined) {
		this.userDefined = userDefined;
	}

	public BigDecimal getTradeTiBenPriTouch() {
		return tradeTiBenPriTouch;
	}

	public void setTradeTiBenPriTouch(BigDecimal tradeTiBenPriTouch) {
		this.tradeTiBenPriTouch = tradeTiBenPriTouch;
	}

	public BigDecimal getHiLoBenPriTouch() {
		return hiLoBenPriTouch;
	}

	public void setHiLoBenPriTouch(BigDecimal hiLoBenPriTouch) {
		this.hiLoBenPriTouch = hiLoBenPriTouch;
	}

	public BigDecimal getTradeTiBenPriEff() {
		return tradeTiBenPriEff;
	}

	public void setTradeTiBenPriEff(BigDecimal tradeTiBenPriEff) {
		this.tradeTiBenPriEff = tradeTiBenPriEff;
	}

	public BigDecimal getHiLoBenPriEff() {
		return hiLoBenPriEff;
	}

	public void setHiLoBenPriEff(BigDecimal hiLoBenPriEff) {
		this.hiLoBenPriEff = hiLoBenPriEff;
	}

	public String getTradeTiBenVenTouch() {
		return tradeTiBenVenTouch;
	}

	public void setTradeTiBenVenTouch(String tradeTiBenVenTouch) {
		this.tradeTiBenVenTouch = tradeTiBenVenTouch;
	}

	public String getHiLoBenVenTouch() {
		return hiLoBenVenTouch;
	}

	public void setHiLoBenVenTouch(String hiLoBenVenTouch) {
		this.hiLoBenVenTouch = hiLoBenVenTouch;
	}

	public String getTradeTiBenVenEff() {
		return tradeTiBenVenEff;
	}

	public void setTradeTiBenVenEff(String tradeTiBenVenEff) {
		this.tradeTiBenVenEff = tradeTiBenVenEff;
	}

	public String getHiLoBenVenEff() {
		return hiLoBenVenEff;
	}

	public void setHiLoBenVenEff(String hiLoBenVenEff) {
		this.hiLoBenVenEff = hiLoBenVenEff;
	}

	public String getTradeTiBenBPSTouch() {
		return tradeTiBenBPSTouch;
	}

	public void setTradeTiBenBPSTouch(String tradeTiBenBPSTouch) {
		this.tradeTiBenBPSTouch = tradeTiBenBPSTouch;
	}

	public String getHiLoBenBPSTouch() {
		return hiLoBenBPSTouch;
	}

	public void setHiLoBenBPSTouch(String hiLoBenBPSTouch) {
		this.hiLoBenBPSTouch = hiLoBenBPSTouch;
	}

	public String getTradeTiBenBPsEff() {
		return tradeTiBenBPsEff;
	}

	public void setTradeTiBenBPsEff(String tradeTiBenBPsEff) {
		this.tradeTiBenBPsEff = tradeTiBenBPsEff;
	}

	public String getHiLoBenBPSEff() {
		return hiLoBenBPSEff;
	}

	public void setHiLoBenBPSEff(String hiLoBenBPSEff) {
		this.hiLoBenBPSEff = hiLoBenBPSEff;
	}

	public String getTradeTiBenSfallValueTouch() {
		return tradeTiBenSfallValueTouch;
	}

	public void setTradeTiBenSfallValueTouch(String tradeTiBenSfallValueTouch) {
		this.tradeTiBenSfallValueTouch = tradeTiBenSfallValueTouch;
	}

	public String getHiLoBenSfallValueTouch() {
		return hiLoBenSfallValueTouch;
	}

	public void setHiLoBenSfallValueTouch(String hiLoBenSfallValueTouch) {
		this.hiLoBenSfallValueTouch = hiLoBenSfallValueTouch;
	}

	public String getTradeTiBenSfallValueEff() {
		return tradeTiBenSfallValueEff;
	}

	public void setTradeTiBenSfallValueEff(String tradeTiBenSfallValueEff) {
		this.tradeTiBenSfallValueEff = tradeTiBenSfallValueEff;
	}

	public String getHiLoBenSfallValueEff() {
		return hiLoBenSfallValueEff;
	}

	public void setHiLoBenSfallValueEff(String hiLoBenSfallValueEff) {
		this.hiLoBenSfallValueEff = hiLoBenSfallValueEff;
	}

	public BigDecimal getXswxBBidPri() {
		return xswxBBidPri;
	}

	public void setXswxBBidPri(BigDecimal xswxBBidPri) {
		this.xswxBBidPri = xswxBBidPri;
	}

	public Long getXswxBBidVol() {
		return xswxBBidVol;
	}

	public void setXswxBBidVol(Long xswxBBidVol) {
		this.xswxBBidVol = xswxBBidVol;
	}

	public BigDecimal getXswxBOffPri() {
		return xswxBOffPri;
	}

	public void setXswxBOffPri(BigDecimal xswxBOffPri) {
		this.xswxBOffPri = xswxBOffPri;
	}

	public Long getXswxBOffVol() {
		return xswxBOffVol;
	}

	public void setXswxBOffVol(Long xswxBOffVol) {
		this.xswxBOffVol = xswxBOffVol;
	}

	public BigDecimal getXswxEffBidPri() {
		return xswxEffBidPri;
	}

	public void setXswxEffBidPri(BigDecimal xswxEffBidPri) {
		this.xswxEffBidPri = xswxEffBidPri;
	}

	public BigDecimal getXswxEffOffPri() {
		return xswxEffOffPri;
	}

	public void setXswxEffOffPri(BigDecimal xswxEffOffPri) {
		this.xswxEffOffPri = xswxEffOffPri;
	}

	public BigDecimal getBateBBidPri() {
		return bateBBidPri;
	}

	public void setBateBBidPri(BigDecimal bateBBidPri) {
		this.bateBBidPri = bateBBidPri;
	}

	public Long getBateBBidVol() {
		return bateBBidVol;
	}

	public void setBateBBidVol(Long bateBBidVol) {
		this.bateBBidVol = bateBBidVol;
	}

	public BigDecimal getBateBOffPri() {
		return bateBOffPri;
	}

	public void setBateBOffPri(BigDecimal bateBOffPri) {
		this.bateBOffPri = bateBOffPri;
	}

	public Long getBateBOffVol() {
		return bateBOffVol;
	}

	public void setBateBOffVol(Long bateBOffVol) {
		this.bateBOffVol = bateBOffVol;
	}

	public BigDecimal getBateEffBidPri() {
		return bateEffBidPri;
	}

	public void setBateEffBidPri(BigDecimal bateEffBidPri) {
		this.bateEffBidPri = bateEffBidPri;
	}

	public BigDecimal getBateEffOffPri() {
		return bateEffOffPri;
	}

	public void setBateEffOffPri(BigDecimal bateEffOffPri) {
		this.bateEffOffPri = bateEffOffPri;
	}

	public BigDecimal getChixBBidPri() {
		return chixBBidPri;
	}

	public void setChixBBidPri(BigDecimal chixBBidPri) {
		this.chixBBidPri = chixBBidPri;
	}

	public Long getChixBBidVol() {
		return chixBBidVol;
	}

	public void setChixBBidVol(Long chixBBidVol) {
		this.chixBBidVol = chixBBidVol;
	}

	public BigDecimal getChixBOffPri() {
		return chixBOffPri;
	}

	public void setChixBOffPri(BigDecimal chixBOffPri) {
		this.chixBOffPri = chixBOffPri;
	}

	public Long getChixBOffVol() {
		return chixBOffVol;
	}

	public void setChixBOffVol(Long chixBOffVol) {
		this.chixBOffVol = chixBOffVol;
	}

	public BigDecimal getChixEffBidPri() {
		return chixEffBidPri;
	}

	public void setChixEffBidPri(BigDecimal chixEffBidPri) {
		this.chixEffBidPri = chixEffBidPri;
	}

	public BigDecimal getChixEffOffPri() {
		return chixEffOffPri;
	}

	public void setChixEffOffPri(BigDecimal chixEffOffPri) {
		this.chixEffOffPri = chixEffOffPri;
	}

	public BigDecimal getTrqxBBidPri() {
		return trqxBBidPri;
	}

	public void setTrqxBBidPri(BigDecimal trqxBBidPri) {
		this.trqxBBidPri = trqxBBidPri;
	}

	public Long getTrqxBBidVol() {
		return trqxBBidVol;
	}

	public void setTrqxBBidVol(Long trqxBBidVol) {
		this.trqxBBidVol = trqxBBidVol;
	}

	public BigDecimal getTrqxBOffPri() {
		return trqxBOffPri;
	}

	public void setTrqxBOffPri(BigDecimal trqxBOffPri) {
		this.trqxBOffPri = trqxBOffPri;
	}

	public Long getTrqxBOffVol() {
		return trqxBOffVol;
	}

	public void setTrqxBOffVol(Long trqxBOffVol) {
		this.trqxBOffVol = trqxBOffVol;
	}

	public BigDecimal getTrqxEffBidPri() {
		return trqxEffBidPri;
	}

	public void setTrqxEffBidPri(BigDecimal trqxEffBidPri) {
		this.trqxEffBidPri = trqxEffBidPri;
	}

	public BigDecimal getTrqxEffOffPri() {
		return trqxEffOffPri;
	}

	public void setTrqxEffOffPri(BigDecimal trqxEffOffPri) {
		this.trqxEffOffPri = trqxEffOffPri;
	}
	
	public BigDecimal getBBidPriXSWXDiffBATE() {
        return BBidPriXSWXDiffBATE;
    }

    public void setBBidPriXSWXDiffBATE(BigDecimal BBidPriXSWXDiffBATE) {
        this.BBidPriXSWXDiffBATE = BBidPriXSWXDiffBATE;
    }

    public BigDecimal getBBidPriXSWXDiffCHIX() {
        return BBidPriXSWXDiffCHIX;
    }

    public void setBBidPriXSWXDiffCHIX(BigDecimal BBidPriXSWXDiffCHIX) {
        this.BBidPriXSWXDiffCHIX = BBidPriXSWXDiffCHIX;
    }

    public BigDecimal getBBidPriXSWXDiffTRQX() {
        return BBidPriXSWXDiffTRQX;
    }

    public void setBBidPriXSWXDiffTRQX(BigDecimal BBidPriXSWXDiffTRQX) {
        this.BBidPriXSWXDiffTRQX = BBidPriXSWXDiffTRQX;
    }

    public BigDecimal getSpreadXswx() {
		return spreadXswx;
	}

	public void setSpreadXswx(BigDecimal BSpreadXswx) {
		this.spreadXswx = BSpreadXswx;
	}

	public BigDecimal getSpreadBate() {
		return spreadBate;
	}

	public void setSpreadBate(BigDecimal BSpreadBate) {
		this.spreadBate = BSpreadBate;
	}

	public BigDecimal getSpreadChix() {
		return spreadChix;
	}

	public void setSpreadChix(BigDecimal BSpreadChix) {
		this.spreadChix = BSpreadChix;
	}

	public BigDecimal getSpreadTrqx() {
		return spreadTrqx;
	}

	public void setSpreadTrqx(BigDecimal BSpreadTrqx) {
		this.spreadTrqx = BSpreadTrqx;
	}

	public String gethighBidPriRanking() {
		return highBidPriRanking;
	}

	public void sethighBidPriRanking(String highBidPriRanking) {
		this.highBidPriRanking = highBidPriRanking;
	}

	public String getlowOffPriRanking() {
		return lowOffPriRanking;
	}

	public void setlowOffPriRanking(String lowOffPriRanking) {
		this.lowOffPriRanking = lowOffPriRanking;
	}

	
	public String gethighBidPriVen() {
		return highBidPriVen;
	}

	public void sethighBidPriVen(String highBidPriVen) {
		this.highBidPriVen = highBidPriVen;
	}
	
	public BigDecimal getAqxeBBidPri() {
		return aqxeBBidPri;
	}

	public void setAqxeBBidPri(BigDecimal aqxeBBidPri) {
		this.aqxeBBidPri = aqxeBBidPri;
	}

	public Long getAqxeBBidVol() {
		return aqxeBBidVol;
	}

	public void setAqxeBBidVol(Long aqxeBBidVol) {
		this.aqxeBBidVol = aqxeBBidVol;
	}

	public BigDecimal getAqxeBOffPri() {
		return aqxeBOffPri;
	}

	public void setAqxeBOffPri(BigDecimal aqxeBOffPri) {
		this.aqxeBOffPri = aqxeBOffPri;
	}

	public Long getAqxeBOffVol() {
		return aqxeBOffVol;
	}

	public void setAqxeBOffVol(Long aqxeBOffVol) {
		this.aqxeBOffVol = aqxeBOffVol;
	}

	public BigDecimal getAqxeEffBidPri() {
		return aqxeEffBidPri;
	}

	public void setAqxeEffBidPri(BigDecimal aqxeEffBidPri) {
		this.aqxeEffBidPri = aqxeEffBidPri;
	}

	public BigDecimal getAqxeEffOffPri() {
		return aqxeEffOffPri;
	}

	public void setAqxeEffOffPri(BigDecimal aqxeEffOffPri) {
		this.aqxeEffOffPri = aqxeEffOffPri;
	}

	public BigDecimal getBBidPriXSWXDiffAQXE() {
		return BBidPriXSWXDiffAQXE;
	}

	public void setBBidPriXSWXDiffAQXE(BigDecimal BBidPriXSWXDiffAQXE) {
		this.BBidPriXSWXDiffAQXE = BBidPriXSWXDiffAQXE;
	}

	public BigDecimal getSpreadAqxe() {
		return spreadAqxe;
	}

	public void setSpreadAqxe(BigDecimal spreadAqxe) {
		this.spreadAqxe = spreadAqxe;
	}

	public String getBusinessMonth() {
		return businessMonth;
	}

	public void setBusinessMonth(String businessMonth) {
		this.businessMonth = businessMonth;
	}

	@Override
	public String toString() {
		return "Ben [businessDate=" + businessDate + ", isin=" + isin + ", tradeID=" + tradeID + ", fileName="
				+ fileName + ", ccy=" + ccy + ", fullName=" + fullName + ", tradeTime=" + tradeTime + ", tradePri="
				+ tradePri + ", tradeQty=" + tradeQty + ", tidm=" + tidm + ", side=" + side + ", tradingNetworkID="
				+ tradingNetworkID + ", trader=" + trader + ", counterparty=" + counterparty + ", orderRef=" + orderRef
				+ ", participant=" + participant + ", portfolioManagerID=" + portfolioManagerID + ", userDefined="
				+ userDefined + ", tradeTiBenPriTouch=" + tradeTiBenPriTouch
				+ ", hiLoBenPriTouch=" + hiLoBenPriTouch + ", tradeTiBenPriEff="
				+ tradeTiBenPriEff + ", hiLoBenPriEff=" + hiLoBenPriEff
				+ ", tradeTiBenVenTouch=" + tradeTiBenVenTouch + ", hiLoBenVenTouch="
				+ hiLoBenVenTouch + ", tradeTiBenVenEff=" + tradeTiBenVenEff
				+ ", hiLoBenVenEff=" + hiLoBenVenEff + ", tradeTiBenBPSTouch="
				+ tradeTiBenBPSTouch + ", hiLoBenBPSTouch=" + hiLoBenBPSTouch
				+ ", tradeTiBenBPsEff=" + tradeTiBenBPsEff + ", hiLoBenBPSEff="
				+ hiLoBenBPSEff + ", tradeTiBenSfallValueTouch="
				+ tradeTiBenSfallValueTouch + ", hiLoBenSfallValueTouch="
				+ hiLoBenSfallValueTouch + ", tradeTiBenSfallValueEff="
				+ tradeTiBenSfallValueEff + ", hiLoBenSfallValueEff="
				+ hiLoBenSfallValueEff + ", xswxBBidPri=" + xswxBBidPri
				+ ", xswxBBidVol=" + xswxBBidVol + ", xswxBOffPri=" + xswxBOffPri
				+ ", xswxBOffVol=" + xswxBOffVol + ", xswxEffBidPri=" + xswxEffBidPri
				+ ", xswxEffOffPri=" + xswxEffOffPri + ", bateBBidPri=" + bateBBidPri
				+ ", bateBBidVol=" + bateBBidVol + ", bateBOffPri=" + bateBOffPri
				+ ", bateBOffVol=" + bateBOffVol + ", bateEffBidPri=" + bateEffBidPri
				+ ", bateEffOffPri=" + bateEffOffPri + ", chixBBidPri=" + chixBBidPri
				+ ", chixBBidVol=" + chixBBidVol + ", chixBOffPri=" + chixBOffPri
				+ ", chixBOffVol=" + chixBOffVol + ", chixEffBidPri=" + chixEffBidPri
				+ ", chixEffOffPri=" + chixEffOffPri + ", trqxBBidPri=" + trqxBBidPri
				+ ", trqxBBidVol=" + trqxBBidVol + ", trqxBOffPri=" + trqxBOffPri
				+ ", trqxBOffVol=" + trqxBOffVol + ", trqxEffBidPri=" + trqxEffBidPri
				+ ", trqxEffOffPri=" + trqxEffOffPri + "]";
	}

	private static BigDecimal setBigDecimal(final String value) {
		if (value != null && !value.isEmpty()) {
			return new BigDecimal(value).setScale(6, RoundingMode.HALF_UP);
		} else {
			return null;
		}
	}
	
	private static Long setLong(final String value) {
		if (value != null && !value.isEmpty()) {
			return Long.valueOf(value);
		} else {
			return null;
		}
	}

}
