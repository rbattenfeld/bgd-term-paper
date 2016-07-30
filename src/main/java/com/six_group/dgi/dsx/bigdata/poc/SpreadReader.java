package com.six_group.dgi.dsx.bigdata.poc;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import scala.collection.Iterator;

public class SpreadReader {
	private final JavaSparkContext _sc;
	private final String _security;
	private final String _folder;
	
	public static void main(String[] args) {  
        final SpreadReader loader = new SpreadReader(getSparkConf(args), args[2], args[3]);
    } 

	private static SparkConf getSparkConf(String[] args) {
		final SparkConf conf = new SparkConf();
        conf.setAppName("Spread Reader");
        conf.setMaster(args[0]);
        conf.set("spark.cassandra.connection.host", args[1]);

//        conf.setMaster("spark://172.17.0.1:7077");
//        conf.set("spark.cassandra.connection.host", "172.17.0.1");
        return conf;
	}
	
	public SpreadReader(final SparkConf conf, final String security, final String folder) {
		_sc = new JavaSparkContext(conf);
		_security = security;
		_folder = folder;
		readSpread();
	}
	
	public void readSpread() {		
//		final JavaRDD<Spread> spreadRdd = javaFunctions(_sc).cassandraTable("swx", "spread", mapRowTo(Spread.class));
//		final JavaPairRDD<Object, Iterable<Spread>> rddY = spreadRdd.groupBy(spread -> spread.getSecurity());
//		System.out.println("Spread row count: " + spreadRdd.count());
//		final SQLContext sqlContext = new SQLContext(_sc.toSparkContext(_sc));
		final HiveContext hiveContext = new HiveContext(JavaSparkContext.toSparkContext(_sc));
		final DataFrame securitiesDF = hiveContext.sql(String.format("SELECT * FROM swx.spread"));
		final String[] securityList = _security.split(";", -1);
		for ( final String security : securityList) {
			try {
//				final String csvFile = String.format("%s/spreads-%s.csv", _folder, security);
				final DataFrame securityDF = securitiesDF.where(String.format("security = '%s'", security));
				final Row[] rows = securityDF.groupBy("security", "origdate").avg("spread").collect();
				for (final Row row : rows) {
//					System.out.println(row.toString());
					final String sec = row.getString(0);
					final Timestamp date = row.getTimestamp(1);
					final BigDecimal avgSpread = row.getDecimal(2);
					System.out.println("sec: " + sec + " date: " + date + " avg spread: " + avgSpread.toPlainString());
				}
//				final Iterator<Row> row = securitiesDF.rdd().toLocalIterator();		
//				while (row.hasNext()) {
//					System.out.println(row.toString());
//					final Row spreadRow = row.next();
//					final String sec = spreadRow.getString(0);
//					final Timestamp date = spreadRow.getTimestamp(1);
//					final String avgSpread = spreadRow.getString(2);
////					System.out.println("sec: " + sec + " date" + date + " avg spread: " + avgSpread);
//				}
				
				
//				securitiesDF.coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").save(csvFile);
//				System.out.println("created csv file: " + csvFile);
			} catch (final Exception ex) {
				System.out.println(ex.getMessage());
			}
	    }
//		for (final Row row : securitiesDF.collect()) {
//			final String security = row.getString(0);
//			final DataFrame securityDF = hiveContext.sql(String.format("SELECT * FROM swx.spread where security='%s'", security));
//			securityDF.groupBy("origdate").avg("spread").show();
//		}
//		for (final Row row : df.collect()) {
//			final String security = row.getString(0);
//			final DataFrame dfSecurity = hiveContext.sql("select * from swx.spread where security = " + security);
//			dfSecurity.avg("spread");
//		}
		
//		df.groupBy("security").avg("spread").sort(df.col("\"avg(spread)\"").desc()).show();
//		df.groupBy("security").avg("spread").show();
		
//		df.groupBy("security").avg("spread").orderBy("avg(spread)").show();
//		df.groupBy("security").avg("spread").show();
//		df.show();
	}
	
}
