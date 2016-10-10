package com.six_group.dgi.dsx.bigdata.poc.benchmark;

public class RunnableMetrics {
	private final Long _totalRecordsProcessed;
	private final Long _totalProcessingTime;

	public RunnableMetrics(final Long totalRecordsProcessed, final Long totalProcessingTime) {
		super();
		_totalRecordsProcessed = totalRecordsProcessed;
		_totalProcessingTime = totalProcessingTime;
	}

	public Long getTotalRecordsProcessed() {
		return _totalRecordsProcessed;
	}
			
	public Long getTotalProcessingTime() {
		return _totalProcessingTime;
	}
}
