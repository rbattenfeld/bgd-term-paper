package com.six_group.dgi.dsx.bigdata.poc.persisting;

import java.util.List;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.mapping.Mapper;
import com.six_group.dgi.dsx.bigdata.poc.quoting.Spread;
import com.six_group.dgi.dsx.bigdata.poc.quoting.Trade;

public class CassandraPersister implements IPreTradePersister {
    private final Mapper<Spread> _mapperSpread;
    private final Mapper<Trade> _mapperTrade;
    
    public CassandraPersister(final Mapper<Spread> mapperSpread, Mapper<Trade> mapperTrade) {
        _mapperSpread = mapperSpread;
        _mapperTrade = mapperTrade;
    }

    public void saveSpreadsCassandra(final List<Spread> spreads) { 
        if (spreads.size() > 0) {
            final BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
            for (final Spread spread: spreads) {
                batch.add(_mapperSpread.saveQuery(spread));
            }
            _mapperSpread.getManager().getSession().execute(batch);
        }
    }
        
    public void saveTradesCassandra(final List<Trade> trades) {
        if (trades.size() > 0) {
            final BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
            for (final Trade trade: trades) {
                batch.add(_mapperTrade.saveQuery(trade));
            }
            _mapperTrade.getManager().getSession().execute(batch);
        }
    }
}
