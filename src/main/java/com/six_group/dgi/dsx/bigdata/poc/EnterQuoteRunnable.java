//package com.six_group.dgi.dsx.bigdata.poc;
//
//import java.math.BigDecimal;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicBoolean;
//
//public class EnterQuoteRunnable implements Runnable {
//	private final BlockingQueue<String> _lineQueue;
//	private final QuoteBooks _books;
//	private AtomicBoolean _isStopped = new AtomicBoolean(false);
//
//	public EnterQuoteRunnable(final QuoteBooks books, final BlockingQueue<String> lineQueue) {
//		_lineQueue = lineQueue;
//		_books = books;
//    }
//
//	public void stop() {
//		_isStopped.set(true);
//	}
//
//	public long getLinesQueued() {
//	    return	_lineQueue.size();
//	}
//	
//	@Override
//	public void run() {
//		try {
//			while (!_isStopped.get()) {
//			    final String line = _lineQueue.poll(1, TimeUnit.SECONDS);
//				if (line != null) {
//					try {
//				      final QuoteLeg leg = new QuoteLeg(
//				        ExtractValueUtil.INSTANCE.getValue(line, "security", true),
//				        ExtractValueUtil.INSTANCE.getValue(line, "side", true), 
//				        Long.valueOf(ExtractValueUtil.INSTANCE.getValue(line, " id", true)),
//						new BigDecimal(ExtractValueUtil.INSTANCE.getValue(line, "price", true)),
//					    Long.valueOf(ExtractValueUtil.INSTANCE.getValue(line, "volume", true)),
//					    ExtractValueUtil.INSTANCE.getValue(line, "EntrySource", false),
//					    ExtractValueUtil.INSTANCE.getValue(line, "BTT", false),
//					    ExtractValueUtil.INSTANCE.getValue(line, "house", false),
//					    ExtractValueUtil.INSTANCE.getValue(line, "User",false),
//					    ExtractValueUtil.INSTANCE.getValue(line, "Trade Book Type", false),
//					    ExtractValueUtil.INSTANCE.getValue(line, "Original Time", true),
//					    ExtractValueUtil.INSTANCE.getValue(line, "Original Date", true),
//					    ExtractValueUtil.INSTANCE.getValue(line, "ember Organisation Code", true));
//     					_books.add(leg);
//					} catch (final Exception ex) {
////						ex.printStackTrace();
//						System.out.println("invalid row" + line);
//					}
//				} 
//			}
//			System.out.println("Stopped DeleteQuoteLeg consumer");
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//	}
//}