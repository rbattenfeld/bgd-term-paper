package com.six_group.dgi.dsx.bigdata.poc.benchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

public class BenchmarkTest {

	@Test
	public void test() {
		Benchmark.getBenchmark("filenamexxxx", "20160701", "LU1202320294,CHF,COSMO PHARM N,25042016142223696,160.9,2,031B61FQGGK02V1P,COPN,Sell,14,PR,1567N,20160425-1397692-P1,CLRXCHZZ,1253O,Pass,160.5,160.5,160.5,160.5,XSWX,CHIX,XSWX,CHIX,-24.860161591050341827221876942,-24.860161591050341827221876942,-24.860161591050341827221876942,-24.860161591050341827221876942,-0.8,-0.8,-0.8,-0.8,160.5,206,160.9,272,160.5,160.9,159.4,70,163.4,79,159.4,163.4,160.5,17,164.7,79,160.5,164.7,159.4,68,167.2,92,159.4,167.2,159.4,68,167.2,92,159.4,167.2");
	}


	@Test
	public void testConnectOracle() throws Exception {				
		Class.forName("oracle.jdbc.driver.OracleDriver");
		try (final Connection connection = DriverManager.getConnection("jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=mpzhldbcdw01.pn.swx)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=PCDW50_PRIMARY.WORLD)))", "tkba2", "Welcome4tkba2")) {
			final Statement stmt = connection.createStatement();  
			final ResultSet rs = stmt.executeQuery("select count(*) as count from cdwadmin.ifs_trade_data");
			while(rs.next())  {
				int rowCount = rs.getInt("count");
				System.out.println("Row count: " + rowCount);
			}
			if (connection != null) {
				System.out.println("You made it, take control your database now!");
			} else {
				System.out.println("Failed to make connection!");
			}			
		}		
	}
}
