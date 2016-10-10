package com.six_group.dgi.dsx.bigdata.poc.toxicity;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class PropertiesHandler {
	public static final String KEY_CASSANDRA_HOSTS = "com.six_group.dgi.dsx.bigdata.poc.toxicity.cassandraHosts";
	public static final String KEY_CASSANDRA_PORT  = "com.six_group.dgi.dsx.bigdata.poc.toxicity.cassandraPort";
	public static final String KEY_MEMBER_LIST     = "com.six_group.dgi.dsx.bigdata.poc.toxicity.memberList";
	public static final String KEY_ORACLE_USER     = "com.six_group.dgi.dsx.bigdata.poc.toxicity.oracleUser";
	public static final String KEY_ORACLE_PWD      = "com.six_group.dgi.dsx.bigdata.poc.toxicity.oraclePwd";
	public static final String KEY_ORACLE_CONN     = "com.six_group.dgi.dsx.bigdata.poc.toxicity.oracleConn";
	public static final String KEY_SELECT_PROD     = "com.six_group.dgi.dsx.bigdata.poc.toxicity.productSql";
	public static final String KEY_SELECT_INSTR    = "com.six_group.dgi.dsx.bigdata.poc.toxicity.instrumentSql";
	public static final String KEY_THREAD_POOL     = "com.six_group.dgi.dsx.bigdata.poc.toxicity.threadPool";
	public static final String KEY_REQ_PER_CONN    = "com.six_group.dgi.dsx.bigdata.poc.toxicity.maxRequestsPerConnection";
	private final Properties _prop;

	private PropertiesHandler(final Properties prop) {
		_prop = prop;
	}

	public static PropertiesHandler load(final String propFile) throws FileNotFoundException, IOException {
		final Properties prop = new Properties();
		prop.load(new FileInputStream(propFile));
		return new PropertiesHandler(prop);
	}
	
	public String getStringValue(final String key) {
		return _prop.getProperty(key);
	}
	
	public Integer getIntValue(final String key) {
		return Integer.valueOf(_prop.getProperty(key));
	}
}
