package com.six_group.dgi.dsx.bigdata.poc;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DeleteQuoteRunnable implements Runnable {
	private final BlockingQueue<DeleteLeg> _queue;
	private final QuoteBooks _books;
	private AtomicBoolean _isStopped = new AtomicBoolean(false);

	public DeleteQuoteRunnable(final QuoteBooks books, final BlockingQueue<DeleteLeg> queue) {
		_books = books;
        _queue = queue;
    }

	public void stop() {
		_isStopped.set(true);
	}

	public long getLinesQueued() {
	    return	_queue.size();
	}
	
	@Override
	public void run() {
		try {
			int count = 0;
			QuoteLeg msg;
			while (!_isStopped.get()) {
			    final DeleteLeg deleteLeg = _queue.poll(1, TimeUnit.SECONDS);
				if (deleteLeg != null) {
					final QuoteBook book = _books.getBook(deleteLeg.getSecurity());
					if (book != null) {
						final QuoteLeg leg = book.findQuote(deleteLeg.getId());
						if (leg != null && leg.getOtherSide() != null) {
							count++;
							if (count % 10000 == 0) {
								System.out.println("Quotes found: " + count );
							}
						    book.delete(deleteLeg.getId());
						    book.delete(leg.getOtherSide().getId());
						}
					}
				}
			}
			System.out.println("Stopped DeleteQuoteLeg consumer");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}