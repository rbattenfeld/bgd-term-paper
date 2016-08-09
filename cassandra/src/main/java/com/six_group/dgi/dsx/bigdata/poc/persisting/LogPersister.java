package com.six_group.dgi.dsx.bigdata.poc.persisting;

import java.util.List;

import com.six_group.dgi.dsx.bigdata.poc.quoting.Spread;
import com.six_group.dgi.dsx.bigdata.poc.quoting.Trade;

public class LogPersister implements IPreTradePersister {

    public void saveSpreadsCassandra(final List<Spread> spreads) {
        for (final Spread spread : spreads) {
            System.out.println(spread);
        }
    }
        
    public void saveTradesCassandra(final List<Trade> trades) {
        for (final Trade trade : trades) {
            System.out.println(trade);
        }
    }

}
