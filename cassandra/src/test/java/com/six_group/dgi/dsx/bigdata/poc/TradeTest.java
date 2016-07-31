package com.six_group.dgi.dsx.bigdata.poc;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;

import com.six_group.dgi.dsx.bigdata.poc.quoting.Trade;

public class TradeTest {

	@Test
	public void test() throws ParseException {
		final SimpleDateFormat timeFormatter = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss.S");
		final String line = "Q1 09:59:42.407 TRADE: security=\"ABBN\" transid=0 id=538313771634 price=22.66 volume=295 value=6684.7 flags=Bi,QM,Q `trdNostro Cross=\"No\" `trdTPI=\"P\" `trdTrade" 
			+ " Sub Type=\"OnExchange\" bidid=171310000271550 `bidTCC=\"-\" `bidOrderType=\"-\" `bidEntrySource=\"-\" `bidOrderDuration=\"-\" `bidBTT=\"-\" `bidReq TPI=\"-\" `bidLiquidity Ind=\"Aggressor\"" 
			+ " *bidMember Organisation Code=\"MLCGB1_281_\" *bidTrade Book Type=\"OnBook\" askid=171310000268618 `askTCC=\"-\" `askOrderType=\"-\" `askEntrySource=\"-\" `askOrderDuration=\"-\" `askBTT=\"-\"" 
			+ " `askReq TPI=\"-\"" 
			+ "`askLiquidity Ind=\"Poster\" *askMember Organisation Code=\"GSIGB_303_\"";
		Trade.getTrade(line, new Date(), 0, timeFormatter);
	}

}
