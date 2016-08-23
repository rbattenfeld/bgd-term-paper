package com.six_group.dgi.dsx.bigdata.poc.benchmark;

import static org.junit.Assert.*;

import org.junit.Test;

public class ExtractValueUtilTest {

    @Test
    public void test() {
        assertTrue("20160425".equals(ExtractValueUtil.INSTANCE.getDateFromFileName("client-trade-benchmark_CLRXCHZZCH_20160425000000.csv")));
    }

}
