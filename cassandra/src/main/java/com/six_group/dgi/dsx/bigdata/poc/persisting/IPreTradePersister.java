package com.six_group.dgi.dsx.bigdata.poc.persisting;

import java.util.List;

import com.six_group.dgi.dsx.bigdata.poc.quoting.Spread;
import com.six_group.dgi.dsx.bigdata.poc.quoting.Trade;

public interface IPreTradePersister {
    void saveSpreadsCassandra(final List<Spread> spreads);
    void saveTradesCassandra(final List<Trade> trades);
}
