package com.six_group.dgi.dsx.bigdata.poc.benchmark;

public class SecurityPartitioner {
	private final int _bucketCount;
	private int _currentBucketPointer = -1;
	
	public SecurityPartitioner(final int threadCount) {
		_bucketCount = threadCount;
	}

	
	public int getNextBucket() {
		_currentBucketPointer++;
		if (_currentBucketPointer >= _bucketCount) {
			_currentBucketPointer = 0;
		}
		return _currentBucketPointer;
	}
}
