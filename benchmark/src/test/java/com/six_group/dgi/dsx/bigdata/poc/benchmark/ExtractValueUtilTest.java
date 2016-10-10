package com.six_group.dgi.dsx.bigdata.poc.benchmark;

import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

public class ExtractValueUtilTest {

    @Test
    public void test() {
        assertTrue("20160425".equals(ExtractValueUtil.INSTANCE.getDateFromFileName("client-trade-benchmark_CLRXCHZZCH_20160425000000.csv")));
    }

    @Test
    public void testPathList() {
    	ExtractValueUtil.INSTANCE.fileList("C:/develop/Documents/bigdata/201605");
    }

    @Test
    public void testAddToMapt() {
    	final Map<BigDecimal, String> decimalMap = new HashMap<>();
    	ExtractValueUtil.INSTANCE.addToMap(decimalMap, "XSWX", BigDecimal.valueOf(0.0045).setScale(6, RoundingMode.HALF_UP));
    	ExtractValueUtil.INSTANCE.addToMap(decimalMap, "BATE", BigDecimal.valueOf(0.0045).setScale(6, RoundingMode.HALF_UP));
    	ExtractValueUtil.INSTANCE.addToMap(decimalMap, "CHIX", BigDecimal.valueOf(0.00045).setScale(6, RoundingMode.HALF_UP));
    	ExtractValueUtil.INSTANCE.addToMap(decimalMap, "TRQX", BigDecimal.valueOf(0.0044).setScale(6, RoundingMode.HALF_UP));
    	
    	final Map<BigDecimal, String> result = new LinkedHashMap<>();

        //sort by value, and reserve, 10,9,8,7,6...
    	decimalMap.entrySet().stream()
                .sorted(Map.Entry.<BigDecimal, String>comparingByKey().reversed())
                .forEachOrdered(x -> result.put(x.getKey(), x.getValue()));
    	System.out.println(result);
    	
    	StringBuffer buf = new StringBuffer();
    	for (Map.Entry<BigDecimal, String> entry : decimalMap.entrySet()) {
			if (buf.length() > 0) {
				buf = buf.append(", ");
			}
			buf.append(entry.getValue());
		}
    	System.out.println(buf.toString());
    }

}
