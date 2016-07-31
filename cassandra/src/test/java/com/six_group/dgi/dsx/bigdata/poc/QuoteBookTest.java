package com.six_group.dgi.dsx.bigdata.poc;

import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;

import org.junit.Test;

import com.six_group.dgi.dsx.bigdata.poc.quoting.QuoteBook;
import com.six_group.dgi.dsx.bigdata.poc.quoting.QuoteLeg;

public class QuoteBookTest {

	@Test
	public void testCalcSpread() {
		final QuoteLeg askLeg = new QuoteLeg();
		final QuoteLeg bidLeg = new QuoteLeg();
		askLeg.setPrice(BigDecimal.valueOf(880.0).setScale(8, BigDecimal.ROUND_HALF_DOWN));
		askLeg.setSide(QuoteLeg.ASK);
		askLeg.setOtherSide(bidLeg);
		bidLeg.setPrice(BigDecimal.valueOf(860.0).setScale(8, BigDecimal.ROUND_HALF_DOWN));
		bidLeg.setSide(QuoteLeg.BID);
		bidLeg.setOtherSide(askLeg);
		final BigDecimal spread = QuoteBook.calculateSpread(askLeg);
		assertTrue("But was: " + spread.toPlainString(), BigDecimal.valueOf(0.02298851).compareTo(spread) == 0);
	}

}
