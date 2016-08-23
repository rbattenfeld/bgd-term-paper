package com.six_group.dgi.dsx.bigdata.poc.benchmark;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "swx", name = "benchmark",
	readConsistency = "QUORUM",
	writeConsistency = "QUORUM",
	caseSensitiveKeyspace = false,
	caseSensitiveTable = false)
public class Benchmark implements Serializable {
	private static final long serialVersionUID = -3338252723406692293L;
	
	@PartitionKey
	private String businessDate;
	
	@ClusteringColumn(0)
	private String isin;

	@ClusteringColumn(1)
	private String tradeID;
	
	private String fileName;
	private String ccy;
	private String fullName;
	private Date tradeTime;
	private BigDecimal tradePrice;
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
	private BigDecimal tradeTimeBenchmarkPriceTouch;
	private BigDecimal hiLoBenchmarkPriceTouch;
	private BigDecimal tradeTimeBenchmarkPriceEffective; 
	private BigDecimal hiLoBenchmarkPriceEffective;
	private String tradeTimeBenchmarkVenueTouch;
	private String hiLoBenchmarkVenueTouch;
	private String tradeTimeBenchmarkVenueEffective;
	private String hiLoBenchmarkVenueEffective;
	private String tradeTimeBenchmarkBPSTouch;
	private String hiLoBenchmarkBPSTouch;
	private String tradeTimeBenchmarkBPsEffective;
	private String hiLoBenchmarkBPSEffective;
	private String tradeTimeBenchmarkShortfallValueTouch;
	private String hiLoBenchmarkShortfallValueTouch;
	private String tradeTimeBenchmarkShortfallValueEffective;
	private String hiLoBenchmarkShortfallValueEffective;
	private BigDecimal xswxBestBidPrice;
	private Long xswxBestBidVolume;
	private BigDecimal xswxBestOfferPrice;
	private Long xswxBestOfferVolume;
	private BigDecimal xswxEffectiveBidPrice;
	private BigDecimal xswxEffectiveOfferPrice;
	private BigDecimal bateBestBidPrice;
	private Long bateBestBidVolume;
	private BigDecimal bateBestOfferPrice;
	private Long bateBestOfferVolume;
	private BigDecimal bateEffectiveBidPrice;
	private BigDecimal bateEffectiveOfferPrice;
	private BigDecimal chixBestBidPrice;
	private Long chixBestBidVolume;
	private BigDecimal chixBestOfferPrice;
	private Long chixBestOfferVolume;
	private BigDecimal chixEffectiveBidPrice;
	private BigDecimal chixEffectiveOfferPrice;
	private BigDecimal trqxBestBidPrice;
	private Long trqxBestBidVolume;
	private BigDecimal trqxBestOfferPrice;
	private Long trqxBestOfferVolume;
	private BigDecimal trqxEffectiveBidPrice;
	private BigDecimal trqxEffectiveOfferPrice;
	private BigDecimal bestBidPriceXSWXDiffBATE;
	private BigDecimal bestBidPriceXSWXDiffCHIX;
	private BigDecimal bestBidPriceXSWXDiffTRQX;
	private BigDecimal spreadXswx;
	private BigDecimal spreadBate;
	private BigDecimal spreadChix;
	private BigDecimal spreadTrqx;
	private String highestBidPriceVenue; 
	private String highestBidPriceRanking; 
	private String lowestOfferPriceRanking; 
    
	public static Benchmark getBenchmark(final String fileName, final String businessDate, final String line) {
		final String[] items = line.split(",", -1);	
		if (items.length == 56) {
			final Benchmark benchmark = new Benchmark();
			benchmark.setBusinessDate(businessDate);
			benchmark.setFileName(fileName);			
			benchmark.setIsin(items[0]);
			benchmark.setCcy(items[1]);
			benchmark.setFullName(items[2]);
			benchmark.setTradeTime(new Date(Long.valueOf(items[3])));
			benchmark.setTradePrice(new BigDecimal(items[4]));
			benchmark.setTradeQty(setLong(items[5]));
			benchmark.setTradeID(items[6]);
			benchmark.setTidm(items[7]);
			benchmark.setSide(items[8]);
			benchmark.setTradingNetworkID(items[9]);
			benchmark.setTrader(items[10]);
			benchmark.setCounterparty(items[11]);
			benchmark.setOrderRef(items[12]);
			benchmark.setParticipant(items[13]);
			benchmark.setPortfolioManagerID(items[14]); 
			benchmark.setUserDefined(items[15]); 
			benchmark.setTradeTimeBenchmarkPriceTouch(setBigDecimal(items[16]));
			benchmark.setHiLoBenchmarkPriceTouch(setBigDecimal(items[17]));
			benchmark.setTradeTimeBenchmarkPriceEffective(setBigDecimal(items[18])); 
			benchmark.setHiLoBenchmarkPriceEffective(setBigDecimal(items[19]));
			benchmark.setTradeTimeBenchmarkVenueTouch(items[20]);
			benchmark.setHiLoBenchmarkVenueTouch(items[21]);
			benchmark.setTradeTimeBenchmarkVenueEffective(items[22]);
			benchmark.setHiLoBenchmarkVenueEffective(items[23]);
			benchmark.setTradeTimeBenchmarkBPSTouch(items[24]);
			benchmark.setHiLoBenchmarkBPSTouch(items[25]);
			benchmark.setTradeTimeBenchmarkBPsEffective(items[26]);
			benchmark.setHiLoBenchmarkBPSEffective(items[27]);
			benchmark.setTradeTimeBenchmarkShortfallValueTouch(items[28]);
			benchmark.setHiLoBenchmarkShortfallValueTouch(items[29]);
			benchmark.setTradeTimeBenchmarkShortfallValueEffective(items[30]);
			benchmark.setHiLoBenchmarkShortfallValueEffective(items[31]);
			benchmark.setXswxBestBidPrice(setBigDecimal(items[32]));
			benchmark.setXswxBestBidVolume(setLong(items[33]));
			benchmark.setXswxBestOfferPrice(setBigDecimal(items[34]));
			benchmark.setXswxBestOfferVolume(setLong(items[35]));
			benchmark.setXswxEffectiveBidPrice(setBigDecimal(items[36]));
			benchmark.setXswxEffectiveOfferPrice(setBigDecimal(items[37]));
			benchmark.setBateBestBidPrice(setBigDecimal(items[38]));
			benchmark.setBateBestBidVolume(setLong(items[39]));
			benchmark.setBateBestOfferPrice(setBigDecimal(items[40]));
			benchmark.setBateBestOfferVolume(setLong(items[41]));
			benchmark.setBateEffectiveBidPrice(setBigDecimal(items[42]));
			benchmark.setBateEffectiveOfferPrice(setBigDecimal(items[43]));
			benchmark.setChixBestBidPrice(setBigDecimal(items[44]));
			benchmark.setChixBestBidVolume(setLong(items[45]));
			benchmark.setChixBestOfferPrice(setBigDecimal(items[46]));
			benchmark.setChixBestOfferVolume(setLong(items[47]));
			benchmark.setChixEffectiveBidPrice(setBigDecimal(items[48]));
			benchmark.setChixEffectiveOfferPrice(setBigDecimal(items[49]));
			benchmark.setTrqxBestBidPrice(setBigDecimal(items[50]));
			benchmark.setTrqxBestBidVolume(setLong(items[51]));
			benchmark.setTrqxBestOfferPrice(setBigDecimal(items[52]));
			benchmark.setTrqxBestOfferVolume(setLong(items[53]));
			benchmark.setTrqxEffectiveBidPrice(setBigDecimal(items[54]));
			benchmark.setTrqxEffectiveOfferPrice(setBigDecimal(items[55]));
			
			BigDecimal bestBid = BigDecimal.ZERO;			
			final Map<String, BigDecimal> bestBidMap = new HashMap<>();	
			if (benchmark.getXswxBestBidPrice() != null) {
				bestBidMap.put("XSWX", benchmark.getXswxBestBidPrice());
				if (bestBid.compareTo(benchmark.getXswxBestBidPrice()) < 0) {
					bestBid = benchmark.getXswxBestBidPrice();
					benchmark.setHighestBidPriceVenue("XSWX");
				}
			}
			
			if (benchmark.getBateBestBidPrice() != null) {
				bestBidMap.put("BATE", benchmark.getBateBestBidPrice());
				if (bestBid.compareTo(benchmark.getBateBestBidPrice()) < 0) {
					bestBid = benchmark.getBateBestBidPrice();
					benchmark.setHighestBidPriceVenue("BATE");
				}
			}
			
			if (benchmark.getChixBestBidPrice() != null) {
				bestBidMap.put("CHIX", benchmark.getChixBestBidPrice());
				if (bestBid.compareTo(benchmark.getChixBestBidPrice()) < 0) {
					bestBid = benchmark.getChixBestBidPrice();
					benchmark.setHighestBidPriceVenue("CHIX");
				}
			}
			
			if (benchmark.getTrqxBestBidPrice() != null) {
				bestBidMap.put("TRQX", benchmark.getTrqxBestBidPrice());
				if (bestBid.compareTo(benchmark.getTrqxBestBidPrice()) < 0) {
					bestBid = benchmark.getTrqxBestBidPrice();
					benchmark.setHighestBidPriceVenue("TRQX");
				}
			}

			final Map<String, BigDecimal> bestOfferMap = new HashMap<>();
			if (benchmark.getXswxBestOfferPrice() != null) {
				bestOfferMap.put("XSWX", benchmark.getXswxBestOfferPrice());
			}
			if (benchmark.getBateBestOfferPrice() != null) {
				bestOfferMap.put("BATE", benchmark.getBateBestOfferPrice());
			}
			if (benchmark.getChixBestOfferPrice() != null) {
				bestOfferMap.put("CHIX", benchmark.getChixBestOfferPrice());
			}
			if (benchmark.getTrqxBestOfferPrice() != null) {
				bestOfferMap.put("TRQX", benchmark.getTrqxBestOfferPrice());
			}
			
			final Map<String, BigDecimal> rankBestBidMap = new LinkedHashMap<>();
			final Map<String, BigDecimal> rankOfferBidMap = new LinkedHashMap<>();
			
			bestBidMap.entrySet().stream()
	            .sorted(Map.Entry.<String, BigDecimal>comparingByValue().reversed())
	            .forEachOrdered(x -> rankBestBidMap.put(x.getKey(), x.getValue()));
			
			bestOfferMap.entrySet().stream()
	            .sorted(Map.Entry.<String, BigDecimal>comparingByValue())
	            .forEachOrdered(x -> rankOfferBidMap.put(x.getKey(), x.getValue()));
			
			benchmark.setHighestBidPriceRanking(rankBestBidMap.keySet().toString());
			benchmark.setLowestOfferPriceRanking(rankOfferBidMap.keySet().toString());
			
			if (rankBestBidMap.isEmpty()) {
				benchmark.setHighestBidPriceRanking("EMPTY");
			}			
			
			if (benchmark.getXswxBestBidPrice() != null) {
				if (bestBid.compareTo(benchmark.getXswxBestBidPrice()) == 0) {
					String highestBidStr = "[XSWX";
					for (String key : rankBestBidMap.keySet()) {
						if (!key.equals("XSWX")) {
							highestBidStr = highestBidStr + ", " + key;
						}
					}
					highestBidStr = highestBidStr + "]";
					benchmark.setHighestBidPriceRanking(highestBidStr);
				}
			}
						
			if (benchmark.getXswxBestBidPrice() != null && benchmark.getXswxBestOfferPrice() != null) {
				benchmark.setSpreadXswx(benchmark.getXswxBestOfferPrice().subtract(benchmark.getXswxBestBidPrice()));
			}
			
			if (benchmark.getBateBestBidPrice() != null && benchmark.getBateBestOfferPrice() != null) {
				benchmark.setSpreadBate(benchmark.getBateBestOfferPrice().subtract(benchmark.getBateBestBidPrice()));
			}
			
			if (benchmark.getChixBestBidPrice() != null && benchmark.getChixBestOfferPrice() != null) {
				benchmark.setSpreadChix(benchmark.getChixBestOfferPrice().subtract(benchmark.getChixBestBidPrice()));
			}
			
			if (benchmark.getTrqxBestBidPrice() != null && benchmark.getTrqxBestOfferPrice() != null) {
				benchmark.setSpreadTrqx(benchmark.getTrqxBestOfferPrice().subtract(benchmark.getTrqxBestBidPrice()));
			}
			
			if (benchmark.getXswxBestBidPrice() != null) {
			    if (benchmark.getBateBestBidPrice() != null) {
			        benchmark.setBestBidPriceXSWXDiffBATE(benchmark.getXswxBestBidPrice().subtract(benchmark.getBateBestBidPrice()));
			    }
			    if (benchmark.getChixBestBidPrice() != null) {
                    benchmark.setBestBidPriceXSWXDiffCHIX(benchmark.getXswxBestBidPrice().subtract(benchmark.getChixBestBidPrice()));
                }
			    if (benchmark.getTrqxBestBidPrice() != null) {
                    benchmark.setBestBidPriceXSWXDiffTRQX(benchmark.getXswxBestBidPrice().subtract(benchmark.getTrqxBestBidPrice()));
                }
			}
			
			return benchmark;
		}
		throw new IllegalArgumentException("Array length mismatch");
	}

	public Benchmark() {
		super();
	}	
	
	public Benchmark(String businessDate, String isin, String tradeID, String fileName, String ccy, String fullName,
			Date tradeTime, BigDecimal tradePrice, Long tradeQty, String tidm, String side, String tradingNetworkID,
			String trader, String counterparty, String orderRef, String participant, String portfolioManagerID,
			String userDefined, BigDecimal tradeTimeBenchmarkPriceTouch, BigDecimal hiLoBenchmarkPriceTouch,
			BigDecimal tradeTimeBenchmarkPriceEffective, BigDecimal hiLoBenchmarkPriceEffective,
			String tradeTimeBenchmarkVenueTouch, String hiLoBenchmarkVenueTouch,
			String tradeTimeBenchmarkVenueEffective, String hiLoBenchmarkVenueEffective,
			String tradeTimeBenchmarkBPSTouch, String hiLoBenchmarkBPSTouch, String tradeTimeBenchmarkBPsEffective,
			String hiLoBenchmarkBPSEffective, String tradeTimeBenchmarkShortfallValueTouch,
			String hiLoBenchmarkShortfallValueTouch, String tradeTimeBenchmarkShortfallValueEffective,
			String hiLoBenchmarkShortfallValueEffective, BigDecimal xswxBestBidPrice, Long xswxBestBidVolume,
			BigDecimal xswxBestOfferPrice, Long xswxBestOfferVolume, BigDecimal xswxEffectiveBidPrice,
			BigDecimal xswxEffectiveOfferPrice, BigDecimal bateBestBidPrice, Long bateBestBidVolume,
			BigDecimal bateBestOfferPrice, Long bateBestOfferVolume, BigDecimal bateEffectiveBidPrice,
			BigDecimal bateEffectiveOfferPrice, BigDecimal chixBestBidPrice, Long chixBestBidVolume,
			BigDecimal chixBestOfferPrice, Long chixBestOfferVolume, BigDecimal chixEffectiveBidPrice,
			BigDecimal chixEffectiveOfferPrice, BigDecimal trqxBestBidPrice, Long trqxBestBidVolume,
			BigDecimal trqxBestOfferPrice, Long trqxBestOfferVolume, BigDecimal trqxEffectiveBidPrice,
			BigDecimal trqxEffectiveOfferPrice) {
		super();
		this.businessDate = businessDate;
		this.isin = isin;
		this.tradeID = tradeID;
		this.fileName = fileName;
		this.ccy = ccy;
		this.fullName = fullName;
		this.tradeTime = tradeTime;
		this.tradePrice = tradePrice;
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
		this.tradeTimeBenchmarkPriceTouch = tradeTimeBenchmarkPriceTouch;
		this.hiLoBenchmarkPriceTouch = hiLoBenchmarkPriceTouch;
		this.tradeTimeBenchmarkPriceEffective = tradeTimeBenchmarkPriceEffective;
		this.hiLoBenchmarkPriceEffective = hiLoBenchmarkPriceEffective;
		this.tradeTimeBenchmarkVenueTouch = tradeTimeBenchmarkVenueTouch;
		this.hiLoBenchmarkVenueTouch = hiLoBenchmarkVenueTouch;
		this.tradeTimeBenchmarkVenueEffective = tradeTimeBenchmarkVenueEffective;
		this.hiLoBenchmarkVenueEffective = hiLoBenchmarkVenueEffective;
		this.tradeTimeBenchmarkBPSTouch = tradeTimeBenchmarkBPSTouch;
		this.hiLoBenchmarkBPSTouch = hiLoBenchmarkBPSTouch;
		this.tradeTimeBenchmarkBPsEffective = tradeTimeBenchmarkBPsEffective;
		this.hiLoBenchmarkBPSEffective = hiLoBenchmarkBPSEffective;
		this.tradeTimeBenchmarkShortfallValueTouch = tradeTimeBenchmarkShortfallValueTouch;
		this.hiLoBenchmarkShortfallValueTouch = hiLoBenchmarkShortfallValueTouch;
		this.tradeTimeBenchmarkShortfallValueEffective = tradeTimeBenchmarkShortfallValueEffective;
		this.hiLoBenchmarkShortfallValueEffective = hiLoBenchmarkShortfallValueEffective;
		this.xswxBestBidPrice = xswxBestBidPrice;
		this.xswxBestBidVolume = xswxBestBidVolume;
		this.xswxBestOfferPrice = xswxBestOfferPrice;
		this.xswxBestOfferVolume = xswxBestOfferVolume;
		this.xswxEffectiveBidPrice = xswxEffectiveBidPrice;
		this.xswxEffectiveOfferPrice = xswxEffectiveOfferPrice;
		this.bateBestBidPrice = bateBestBidPrice;
		this.bateBestBidVolume = bateBestBidVolume;
		this.bateBestOfferPrice = bateBestOfferPrice;
		this.bateBestOfferVolume = bateBestOfferVolume;
		this.bateEffectiveBidPrice = bateEffectiveBidPrice;
		this.bateEffectiveOfferPrice = bateEffectiveOfferPrice;
		this.chixBestBidPrice = chixBestBidPrice;
		this.chixBestBidVolume = chixBestBidVolume;
		this.chixBestOfferPrice = chixBestOfferPrice;
		this.chixBestOfferVolume = chixBestOfferVolume;
		this.chixEffectiveBidPrice = chixEffectiveBidPrice;
		this.chixEffectiveOfferPrice = chixEffectiveOfferPrice;
		this.trqxBestBidPrice = trqxBestBidPrice;
		this.trqxBestBidVolume = trqxBestBidVolume;
		this.trqxBestOfferPrice = trqxBestOfferPrice;
		this.trqxBestOfferVolume = trqxBestOfferVolume;
		this.trqxEffectiveBidPrice = trqxEffectiveBidPrice;
		this.trqxEffectiveOfferPrice = trqxEffectiveOfferPrice;
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

	public BigDecimal getTradePrice() {
		return tradePrice;
	}

	public void setTradePrice(BigDecimal tradePrice) {
		this.tradePrice = tradePrice;
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

	public BigDecimal getTradeTimeBenchmarkPriceTouch() {
		return tradeTimeBenchmarkPriceTouch;
	}

	public void setTradeTimeBenchmarkPriceTouch(BigDecimal tradeTimeBenchmarkPriceTouch) {
		this.tradeTimeBenchmarkPriceTouch = tradeTimeBenchmarkPriceTouch;
	}

	public BigDecimal getHiLoBenchmarkPriceTouch() {
		return hiLoBenchmarkPriceTouch;
	}

	public void setHiLoBenchmarkPriceTouch(BigDecimal hiLoBenchmarkPriceTouch) {
		this.hiLoBenchmarkPriceTouch = hiLoBenchmarkPriceTouch;
	}

	public BigDecimal getTradeTimeBenchmarkPriceEffective() {
		return tradeTimeBenchmarkPriceEffective;
	}

	public void setTradeTimeBenchmarkPriceEffective(BigDecimal tradeTimeBenchmarkPriceEffective) {
		this.tradeTimeBenchmarkPriceEffective = tradeTimeBenchmarkPriceEffective;
	}

	public BigDecimal getHiLoBenchmarkPriceEffective() {
		return hiLoBenchmarkPriceEffective;
	}

	public void setHiLoBenchmarkPriceEffective(BigDecimal hiLoBenchmarkPriceEffective) {
		this.hiLoBenchmarkPriceEffective = hiLoBenchmarkPriceEffective;
	}

	public String getTradeTimeBenchmarkVenueTouch() {
		return tradeTimeBenchmarkVenueTouch;
	}

	public void setTradeTimeBenchmarkVenueTouch(String tradeTimeBenchmarkVenueTouch) {
		this.tradeTimeBenchmarkVenueTouch = tradeTimeBenchmarkVenueTouch;
	}

	public String getHiLoBenchmarkVenueTouch() {
		return hiLoBenchmarkVenueTouch;
	}

	public void setHiLoBenchmarkVenueTouch(String hiLoBenchmarkVenueTouch) {
		this.hiLoBenchmarkVenueTouch = hiLoBenchmarkVenueTouch;
	}

	public String getTradeTimeBenchmarkVenueEffective() {
		return tradeTimeBenchmarkVenueEffective;
	}

	public void setTradeTimeBenchmarkVenueEffective(String tradeTimeBenchmarkVenueEffective) {
		this.tradeTimeBenchmarkVenueEffective = tradeTimeBenchmarkVenueEffective;
	}

	public String getHiLoBenchmarkVenueEffective() {
		return hiLoBenchmarkVenueEffective;
	}

	public void setHiLoBenchmarkVenueEffective(String hiLoBenchmarkVenueEffective) {
		this.hiLoBenchmarkVenueEffective = hiLoBenchmarkVenueEffective;
	}

	public String getTradeTimeBenchmarkBPSTouch() {
		return tradeTimeBenchmarkBPSTouch;
	}

	public void setTradeTimeBenchmarkBPSTouch(String tradeTimeBenchmarkBPSTouch) {
		this.tradeTimeBenchmarkBPSTouch = tradeTimeBenchmarkBPSTouch;
	}

	public String getHiLoBenchmarkBPSTouch() {
		return hiLoBenchmarkBPSTouch;
	}

	public void setHiLoBenchmarkBPSTouch(String hiLoBenchmarkBPSTouch) {
		this.hiLoBenchmarkBPSTouch = hiLoBenchmarkBPSTouch;
	}

	public String getTradeTimeBenchmarkBPsEffective() {
		return tradeTimeBenchmarkBPsEffective;
	}

	public void setTradeTimeBenchmarkBPsEffective(String tradeTimeBenchmarkBPsEffective) {
		this.tradeTimeBenchmarkBPsEffective = tradeTimeBenchmarkBPsEffective;
	}

	public String getHiLoBenchmarkBPSEffective() {
		return hiLoBenchmarkBPSEffective;
	}

	public void setHiLoBenchmarkBPSEffective(String hiLoBenchmarkBPSEffective) {
		this.hiLoBenchmarkBPSEffective = hiLoBenchmarkBPSEffective;
	}

	public String getTradeTimeBenchmarkShortfallValueTouch() {
		return tradeTimeBenchmarkShortfallValueTouch;
	}

	public void setTradeTimeBenchmarkShortfallValueTouch(String tradeTimeBenchmarkShortfallValueTouch) {
		this.tradeTimeBenchmarkShortfallValueTouch = tradeTimeBenchmarkShortfallValueTouch;
	}

	public String getHiLoBenchmarkShortfallValueTouch() {
		return hiLoBenchmarkShortfallValueTouch;
	}

	public void setHiLoBenchmarkShortfallValueTouch(String hiLoBenchmarkShortfallValueTouch) {
		this.hiLoBenchmarkShortfallValueTouch = hiLoBenchmarkShortfallValueTouch;
	}

	public String getTradeTimeBenchmarkShortfallValueEffective() {
		return tradeTimeBenchmarkShortfallValueEffective;
	}

	public void setTradeTimeBenchmarkShortfallValueEffective(String tradeTimeBenchmarkShortfallValueEffective) {
		this.tradeTimeBenchmarkShortfallValueEffective = tradeTimeBenchmarkShortfallValueEffective;
	}

	public String getHiLoBenchmarkShortfallValueEffective() {
		return hiLoBenchmarkShortfallValueEffective;
	}

	public void setHiLoBenchmarkShortfallValueEffective(String hiLoBenchmarkShortfallValueEffective) {
		this.hiLoBenchmarkShortfallValueEffective = hiLoBenchmarkShortfallValueEffective;
	}

	public BigDecimal getXswxBestBidPrice() {
		return xswxBestBidPrice;
	}

	public void setXswxBestBidPrice(BigDecimal xswxBestBidPrice) {
		this.xswxBestBidPrice = xswxBestBidPrice;
	}

	public Long getXswxBestBidVolume() {
		return xswxBestBidVolume;
	}

	public void setXswxBestBidVolume(Long xswxBestBidVolume) {
		this.xswxBestBidVolume = xswxBestBidVolume;
	}

	public BigDecimal getXswxBestOfferPrice() {
		return xswxBestOfferPrice;
	}

	public void setXswxBestOfferPrice(BigDecimal xswxBestOfferPrice) {
		this.xswxBestOfferPrice = xswxBestOfferPrice;
	}

	public Long getXswxBestOfferVolume() {
		return xswxBestOfferVolume;
	}

	public void setXswxBestOfferVolume(Long xswxBestOfferVolume) {
		this.xswxBestOfferVolume = xswxBestOfferVolume;
	}

	public BigDecimal getXswxEffectiveBidPrice() {
		return xswxEffectiveBidPrice;
	}

	public void setXswxEffectiveBidPrice(BigDecimal xswxEffectiveBidPrice) {
		this.xswxEffectiveBidPrice = xswxEffectiveBidPrice;
	}

	public BigDecimal getXswxEffectiveOfferPrice() {
		return xswxEffectiveOfferPrice;
	}

	public void setXswxEffectiveOfferPrice(BigDecimal xswxEffectiveOfferPrice) {
		this.xswxEffectiveOfferPrice = xswxEffectiveOfferPrice;
	}

	public BigDecimal getBateBestBidPrice() {
		return bateBestBidPrice;
	}

	public void setBateBestBidPrice(BigDecimal bateBestBidPrice) {
		this.bateBestBidPrice = bateBestBidPrice;
	}

	public Long getBateBestBidVolume() {
		return bateBestBidVolume;
	}

	public void setBateBestBidVolume(Long bateBestBidVolume) {
		this.bateBestBidVolume = bateBestBidVolume;
	}

	public BigDecimal getBateBestOfferPrice() {
		return bateBestOfferPrice;
	}

	public void setBateBestOfferPrice(BigDecimal bateBestOfferPrice) {
		this.bateBestOfferPrice = bateBestOfferPrice;
	}

	public Long getBateBestOfferVolume() {
		return bateBestOfferVolume;
	}

	public void setBateBestOfferVolume(Long bateBestOfferVolume) {
		this.bateBestOfferVolume = bateBestOfferVolume;
	}

	public BigDecimal getBateEffectiveBidPrice() {
		return bateEffectiveBidPrice;
	}

	public void setBateEffectiveBidPrice(BigDecimal bateEffectiveBidPrice) {
		this.bateEffectiveBidPrice = bateEffectiveBidPrice;
	}

	public BigDecimal getBateEffectiveOfferPrice() {
		return bateEffectiveOfferPrice;
	}

	public void setBateEffectiveOfferPrice(BigDecimal bateEffectiveOfferPrice) {
		this.bateEffectiveOfferPrice = bateEffectiveOfferPrice;
	}

	public BigDecimal getChixBestBidPrice() {
		return chixBestBidPrice;
	}

	public void setChixBestBidPrice(BigDecimal chixBestBidPrice) {
		this.chixBestBidPrice = chixBestBidPrice;
	}

	public Long getChixBestBidVolume() {
		return chixBestBidVolume;
	}

	public void setChixBestBidVolume(Long chixBestBidVolume) {
		this.chixBestBidVolume = chixBestBidVolume;
	}

	public BigDecimal getChixBestOfferPrice() {
		return chixBestOfferPrice;
	}

	public void setChixBestOfferPrice(BigDecimal chixBestOfferPrice) {
		this.chixBestOfferPrice = chixBestOfferPrice;
	}

	public Long getChixBestOfferVolume() {
		return chixBestOfferVolume;
	}

	public void setChixBestOfferVolume(Long chixBestOfferVolume) {
		this.chixBestOfferVolume = chixBestOfferVolume;
	}

	public BigDecimal getChixEffectiveBidPrice() {
		return chixEffectiveBidPrice;
	}

	public void setChixEffectiveBidPrice(BigDecimal chixEffectiveBidPrice) {
		this.chixEffectiveBidPrice = chixEffectiveBidPrice;
	}

	public BigDecimal getChixEffectiveOfferPrice() {
		return chixEffectiveOfferPrice;
	}

	public void setChixEffectiveOfferPrice(BigDecimal chixEffectiveOfferPrice) {
		this.chixEffectiveOfferPrice = chixEffectiveOfferPrice;
	}

	public BigDecimal getTrqxBestBidPrice() {
		return trqxBestBidPrice;
	}

	public void setTrqxBestBidPrice(BigDecimal trqxBestBidPrice) {
		this.trqxBestBidPrice = trqxBestBidPrice;
	}

	public Long getTrqxBestBidVolume() {
		return trqxBestBidVolume;
	}

	public void setTrqxBestBidVolume(Long trqxBestBidVolume) {
		this.trqxBestBidVolume = trqxBestBidVolume;
	}

	public BigDecimal getTrqxBestOfferPrice() {
		return trqxBestOfferPrice;
	}

	public void setTrqxBestOfferPrice(BigDecimal trqxBestOfferPrice) {
		this.trqxBestOfferPrice = trqxBestOfferPrice;
	}

	public Long getTrqxBestOfferVolume() {
		return trqxBestOfferVolume;
	}

	public void setTrqxBestOfferVolume(Long trqxBestOfferVolume) {
		this.trqxBestOfferVolume = trqxBestOfferVolume;
	}

	public BigDecimal getTrqxEffectiveBidPrice() {
		return trqxEffectiveBidPrice;
	}

	public void setTrqxEffectiveBidPrice(BigDecimal trqxEffectiveBidPrice) {
		this.trqxEffectiveBidPrice = trqxEffectiveBidPrice;
	}

	public BigDecimal getTrqxEffectiveOfferPrice() {
		return trqxEffectiveOfferPrice;
	}

	public void setTrqxEffectiveOfferPrice(BigDecimal trqxEffectiveOfferPrice) {
		this.trqxEffectiveOfferPrice = trqxEffectiveOfferPrice;
	}
	
	public BigDecimal getBestBidPriceXSWXDiffBATE() {
        return bestBidPriceXSWXDiffBATE;
    }

    public void setBestBidPriceXSWXDiffBATE(BigDecimal bestBidPriceXSWXDiffBATE) {
        this.bestBidPriceXSWXDiffBATE = bestBidPriceXSWXDiffBATE;
    }

    public BigDecimal getBestBidPriceXSWXDiffCHIX() {
        return bestBidPriceXSWXDiffCHIX;
    }

    public void setBestBidPriceXSWXDiffCHIX(BigDecimal bestBidPriceXSWXDiffCHIX) {
        this.bestBidPriceXSWXDiffCHIX = bestBidPriceXSWXDiffCHIX;
    }

    public BigDecimal getBestBidPriceXSWXDiffTRQX() {
        return bestBidPriceXSWXDiffTRQX;
    }

    public void setBestBidPriceXSWXDiffTRQX(BigDecimal bestBidPriceXSWXDiffTRQX) {
        this.bestBidPriceXSWXDiffTRQX = bestBidPriceXSWXDiffTRQX;
    }

    public BigDecimal getSpreadXswx() {
		return spreadXswx;
	}

	public void setSpreadXswx(BigDecimal bestSpreadXswx) {
		this.spreadXswx = bestSpreadXswx;
	}

	public BigDecimal getSpreadBate() {
		return spreadBate;
	}

	public void setSpreadBate(BigDecimal bestSpreadBate) {
		this.spreadBate = bestSpreadBate;
	}

	public BigDecimal getSpreadChix() {
		return spreadChix;
	}

	public void setSpreadChix(BigDecimal bestSpreadChix) {
		this.spreadChix = bestSpreadChix;
	}

	public BigDecimal getSpreadTrqx() {
		return spreadTrqx;
	}

	public void setSpreadTrqx(BigDecimal bestSpreadTrqx) {
		this.spreadTrqx = bestSpreadTrqx;
	}

	public String getHighestBidPriceRanking() {
		return highestBidPriceRanking;
	}

	public void setHighestBidPriceRanking(String highestBidPriceRanking) {
		this.highestBidPriceRanking = highestBidPriceRanking;
	}

	public String getLowestOfferPriceRanking() {
		return lowestOfferPriceRanking;
	}

	public void setLowestOfferPriceRanking(String lowestOfferPriceRanking) {
		this.lowestOfferPriceRanking = lowestOfferPriceRanking;
	}

	
	public String getHighestBidPriceVenue() {
		return highestBidPriceVenue;
	}

	public void setHighestBidPriceVenue(String highestBidPriceVenue) {
		this.highestBidPriceVenue = highestBidPriceVenue;
	}

	@Override
	public String toString() {
		return "Benchmark [businessDate=" + businessDate + ", isin=" + isin + ", tradeID=" + tradeID + ", fileName="
				+ fileName + ", ccy=" + ccy + ", fullName=" + fullName + ", tradeTime=" + tradeTime + ", tradePrice="
				+ tradePrice + ", tradeQty=" + tradeQty + ", tidm=" + tidm + ", side=" + side + ", tradingNetworkID="
				+ tradingNetworkID + ", trader=" + trader + ", counterparty=" + counterparty + ", orderRef=" + orderRef
				+ ", participant=" + participant + ", portfolioManagerID=" + portfolioManagerID + ", userDefined="
				+ userDefined + ", tradeTimeBenchmarkPriceTouch=" + tradeTimeBenchmarkPriceTouch
				+ ", hiLoBenchmarkPriceTouch=" + hiLoBenchmarkPriceTouch + ", tradeTimeBenchmarkPriceEffective="
				+ tradeTimeBenchmarkPriceEffective + ", hiLoBenchmarkPriceEffective=" + hiLoBenchmarkPriceEffective
				+ ", tradeTimeBenchmarkVenueTouch=" + tradeTimeBenchmarkVenueTouch + ", hiLoBenchmarkVenueTouch="
				+ hiLoBenchmarkVenueTouch + ", tradeTimeBenchmarkVenueEffective=" + tradeTimeBenchmarkVenueEffective
				+ ", hiLoBenchmarkVenueEffective=" + hiLoBenchmarkVenueEffective + ", tradeTimeBenchmarkBPSTouch="
				+ tradeTimeBenchmarkBPSTouch + ", hiLoBenchmarkBPSTouch=" + hiLoBenchmarkBPSTouch
				+ ", tradeTimeBenchmarkBPsEffective=" + tradeTimeBenchmarkBPsEffective + ", hiLoBenchmarkBPSEffective="
				+ hiLoBenchmarkBPSEffective + ", tradeTimeBenchmarkShortfallValueTouch="
				+ tradeTimeBenchmarkShortfallValueTouch + ", hiLoBenchmarkShortfallValueTouch="
				+ hiLoBenchmarkShortfallValueTouch + ", tradeTimeBenchmarkShortfallValueEffective="
				+ tradeTimeBenchmarkShortfallValueEffective + ", hiLoBenchmarkShortfallValueEffective="
				+ hiLoBenchmarkShortfallValueEffective + ", xswxBestBidPrice=" + xswxBestBidPrice
				+ ", xswxBestBidVolume=" + xswxBestBidVolume + ", xswxBestOfferPrice=" + xswxBestOfferPrice
				+ ", xswxBestOfferVolume=" + xswxBestOfferVolume + ", xswxEffectiveBidPrice=" + xswxEffectiveBidPrice
				+ ", xswxEffectiveOfferPrice=" + xswxEffectiveOfferPrice + ", bateBestBidPrice=" + bateBestBidPrice
				+ ", bateBestBidVolume=" + bateBestBidVolume + ", bateBestOfferPrice=" + bateBestOfferPrice
				+ ", bateBestOfferVolume=" + bateBestOfferVolume + ", bateEffectiveBidPrice=" + bateEffectiveBidPrice
				+ ", bateEffectiveOfferPrice=" + bateEffectiveOfferPrice + ", chixBestBidPrice=" + chixBestBidPrice
				+ ", chixBestBidVolume=" + chixBestBidVolume + ", chixBestOfferPrice=" + chixBestOfferPrice
				+ ", chixBestOfferVolume=" + chixBestOfferVolume + ", chixEffectiveBidPrice=" + chixEffectiveBidPrice
				+ ", chixEffectiveOfferPrice=" + chixEffectiveOfferPrice + ", trqxBestBidPrice=" + trqxBestBidPrice
				+ ", trqxBestBidVolume=" + trqxBestBidVolume + ", trqxBestOfferPrice=" + trqxBestOfferPrice
				+ ", trqxBestOfferVolume=" + trqxBestOfferVolume + ", trqxEffectiveBidPrice=" + trqxEffectiveBidPrice
				+ ", trqxEffectiveOfferPrice=" + trqxEffectiveOfferPrice + "]";
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
