package com.six_group.dgi.dsx.bigdata.poc;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.six_group.dgi.dsx.bigdata.poc.parsing.ExtractValueUtil;

public class SparkDemoTest {
	private static final String _STR = "4004035 : Q2 09:19:56.940 DELET: security=\"SDAAH\" transid=0 side=bid id=171310002645987 trader=- reason=2 *Member Organisation Code=\"xxxZH_124_\"";

	@Test
	public void testGetValue() {
		assertEquals("SDAAH", ExtractValueUtil.INSTANCE.getValue(_STR, "security", true));
		assertEquals("171310002645987", ExtractValueUtil.INSTANCE.getValue(_STR, " id",true));
		assertEquals("bid", ExtractValueUtil.INSTANCE.getValue(_STR, "side", true));
		assertEquals("xxxZH_124_", ExtractValueUtil.INSTANCE.getValue(_STR, "Member Organisation Code", true));
		
	}

//	@Test
//	public void testLoad() {
//		PreTradeLoader.loadFavFile(null, "/home/rbattenfeld/Documents/demo.fav.txt");
//	}
}
