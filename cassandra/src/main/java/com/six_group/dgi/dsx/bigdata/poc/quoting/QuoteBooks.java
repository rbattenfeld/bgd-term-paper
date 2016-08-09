package com.six_group.dgi.dsx.bigdata.poc.quoting;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class QuoteBooks {
	private final ConcurrentMap<String, QuoteBook> _bookMap = new ConcurrentHashMap<>();
	
    public void add(final QuoteLeg leg) {
    	final QuoteBook book =_bookMap.get(leg.getSecurity());
    	if (book != null) {
    		book.add(leg);
    	} else {
    		final QuoteBook newBook = new QuoteBook(leg.getSecurity());
    		newBook.add(leg);
    		_bookMap.put(leg.getSecurity(), newBook);
    	}
    }

    public void delete(final String security, final Long id) {
    	final QuoteBook book =_bookMap.get(security);
    	if (book != null) {
    		book.delete(id);
    	} else {
    		// TODO
    	}
    }

    public QuoteBook getBook(final String security) {
    	return _bookMap.get(security);
    }
    
    public void printBookStatistic() {
    	int totalSize = 0;
    	int totalMatchedQuotes = 0;
    	for (final QuoteBook book : _bookMap.values()) {
    		totalSize += book.getAskSize() + book.getBidSize();
    		totalMatchedQuotes += book.getMatchingCount();
    	}
    	System.out.println(String.format("Total matched quotes       : %d", totalMatchedQuotes));
        System.out.println(String.format("Total legs without del msg : %d", totalSize));
    }
}
