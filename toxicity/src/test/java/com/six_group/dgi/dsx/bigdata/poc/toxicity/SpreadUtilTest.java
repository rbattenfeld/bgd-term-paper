package com.six_group.dgi.dsx.bigdata.poc.toxicity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

import org.junit.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

public class SpreadUtilTest {
	

	@Test
	public void testCalculateBps() {
		assertEquals(Double.valueOf(40.99590), SpreadUtil.INSTANCE.calculateBps(Double.valueOf(100.01), Double.valueOf(99.6), 10000));
	}

	@Test
	public void testTradeJDBC() throws Exception {
//		final Session _session;
//		final MappingManager _mapperManager;
//		final Mapper<TradeFact> _mapperTrade;
		
		Connection conn = null;
		Class.forName("oracle.jdbc.driver.OracleDriver");
		try {
			conn = DriverManager.getConnection("jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=mpzhldbcdw01.pn.swx)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=PCDW50_PRIMARY.WORLD)))", "tkba2", "Welcome4tkba2");
			String selectSQL = "SELECT * FROM mrtadmin.trade_fact WHERE traded_date_dim_key = 20160801";
			PreparedStatement preparedStatement = conn.prepareStatement(selectSQL);
//			preparedStatement.setInt(1, 1001);
			ResultSet rs = preparedStatement.executeQuery(selectSQL );
			
			ResultSetMetaData metaData = rs.getMetaData();  // Get meta data of result set
            int nCols = metaData.getColumnCount();    // Get # fields (columns) in result
            for (int i = 1; i<=nCols; i++) {
            	System.out.println(metaData.getColumnName(i));
            }
			
			
			while (rs.next()) {
				final BigDecimal price = rs.getBigDecimal("PRICE");
				final Long volume = rs.getLong("volume");
				final String tradeID = rs.getString("trade_id");
				final Long dateDate = rs.getLong("traded_date_dim_key");
				final Timestamp timestamp = rs.getTimestamp("traded_timestamp");
				final Long buyParticipantID = rs.getLong("buy_participant_dim_key");
				final Long sellParticipantID = rs.getLong("sell_participant_dim_key");
				final Long instrumentID = rs.getLong("instrument_dim_key");
				final Long buyTradingBehaviorID = rs.getLong("buy_trading_behaviour_dim_key");
				final Long sellTradingBehaviorID = rs.getLong("sell_trading_behaviour_dim_key");
			}
			
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}
	
	@Test
	public void testBestBidFloor() {
		final long currentTime = System.currentTimeMillis();
		final Calendar cal = Calendar.getInstance();
		final TradeFact trade = new TradeFact();
		trade.setTradedTimestamp(new Date(currentTime));
		trade.setPrice(100.10);
		
		final TreeMap<Long, List<DailyEntireSpread>> spreadMultiMap = new TreeMap<>();
		putToMap(currentTime - 10015, 100.15, spreadMultiMap);
		putToMap(currentTime - 10012, 100.12, spreadMultiMap);
		putToMap(currentTime - 10011, 100.11, spreadMultiMap);
		putToMap(currentTime - 10010, 100.10, spreadMultiMap);
		putToMap(currentTime - 10009, 100.09, spreadMultiMap);
		putToMap(currentTime - 10008, 100.08, spreadMultiMap);
		putToMap(currentTime - 10007, 100.07, spreadMultiMap);
		putToMap(currentTime - 10006, 100.06, spreadMultiMap);
		putToMap(currentTime - 10004, 100.04, spreadMultiMap);
		putToMap(currentTime - 10002, 100.02, spreadMultiMap);
		putToMap(currentTime - 10001, 100.01, spreadMultiMap);		
		putToMap(currentTime - 10000, 100.0, spreadMultiMap);
		
		executeTest(cal, trade, spreadMultiMap, -10000, 100.0);
		executeTest(cal, trade, spreadMultiMap, -10001, 100.01);
		executeTest(cal, trade, spreadMultiMap, -10002, 100.02);
		executeTest(cal, trade, spreadMultiMap, -10003, 100.04);
		executeTest(cal, trade, spreadMultiMap, -10004, 100.04);
		executeTest(cal, trade, spreadMultiMap, -10005, 100.06);
		executeTest(cal, trade, spreadMultiMap, -10006, 100.06);
		executeTest(cal, trade, spreadMultiMap, -10007, 100.07);
		executeTest(cal, trade, spreadMultiMap, -10008, 100.08);
		executeTest(cal, trade, spreadMultiMap, -10009, 100.09);
		executeTest(cal, trade, spreadMultiMap, -10010, 100.10);
		executeTest(cal, trade, spreadMultiMap, -10011, 100.11);
		executeTest(cal, trade, spreadMultiMap, -10012, 100.12);

		executeTest(cal, trade, spreadMultiMap, -10013, 100.15);
		executeTest(cal, trade, spreadMultiMap, -10014, 100.15);
		executeTest(cal, trade, spreadMultiMap, -10015, 100.15);
		executeTest(cal, trade, spreadMultiMap, -10016, null);
		
	}

	private void executeTest(final Calendar cal, final TradeFact trade, final TreeMap<Long, List<DailyEntireSpread>> spreadMultiMap, final int timeSlice, final Double expectedPrice) {
		final Double floorPrice = SpreadUtil.INSTANCE.getBpsBestBidFloorPrice(cal, trade, spreadMultiMap, timeSlice, timeSlice + 200);
		if (expectedPrice != null) {
			assertTrue(Double.valueOf(expectedPrice).compareTo(floorPrice) == 0);
		} else {
			assertNull(floorPrice);
		}
	}
	
	private void putToMap(final long key, final double bestBidPrice, final TreeMap<Long, List<DailyEntireSpread>> spreadMultiMap) {
		final List<DailyEntireSpread> spreadList = new ArrayList<>();
		final DailyEntireSpread spread = new DailyEntireSpread();
		spread.setLastBestBidPrice(bestBidPrice);
		spreadList.add(spread);
		spreadMultiMap.put(key, spreadList);
	}
}
