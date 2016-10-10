package com.six_group.dgi.dsx.bigdata.poc.benchmark;

public class BenchmarkRunnableItem {
	private final String _fileName;
	private final String _currentDateAsString;
	private final String _line;

	public BenchmarkRunnableItem(final String fileName, final String currentDateAsString, final String line) {
		super();
		_fileName = fileName;
		_currentDateAsString = currentDateAsString;
		_line = line;
	}

	public String getFileName() {
		return _fileName;
	}
	
	public String getLine() {
		return _line;
	}

	public String getCurrentDateAsString() {
		return _currentDateAsString;
	}
}
