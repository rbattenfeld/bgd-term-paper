package com.six_group.dgi.dsx.bigdata.poc.benchmark;

import java.util.HashMap;
import java.util.Map;

public class SecurityPartitioner {
	private final Map<String, Integer> _securityToBucketMap = new HashMap<>();
	private final int _bucketCount;
	private int _currentBucketPointer = -1;
	
	public SecurityPartitioner(final int threadCount) {
		_bucketCount = threadCount;
	}

	public int getBucket(final String security) {
		final Integer bucketNo = _securityToBucketMap.get(security);
		if (bucketNo != null) {
			return bucketNo;
		} else {
			_currentBucketPointer++;
			if (_currentBucketPointer >= _bucketCount) {
				_currentBucketPointer = 0;
			}
			_securityToBucketMap.put(security, _currentBucketPointer);
			return _currentBucketPointer;
		}
	}
}
