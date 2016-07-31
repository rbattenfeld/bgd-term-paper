package com.six_group.dgi.dsx.bigdata.poc;

public enum ExtractValueUtil {
	INSTANCE;
	
	public String getValue(final String str, final String key, final boolean throwException) {
		final int begin = str.indexOf(key);
		if (begin >=0 ) {
			final int end = str.indexOf("=", begin);
			if (end > begin) {
				if (str.charAt(end + 1) == '"') {
					final int qouteEnd = str.indexOf('"', end + 2);
					return str.substring(end + 2, qouteEnd);
				} else {
					final int valueEnd = str.indexOf(' ', end);
					return str.substring(end + 1, valueEnd);
				}
			}
		}
		if (throwException) {
			throw new RuntimeException();
		} else {
			return null;
		}
	}
	
	public void sleep(final long sleepTime) {
		try {
			Thread.currentThread();
			Thread.sleep(sleepTime);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}			
	}
}
