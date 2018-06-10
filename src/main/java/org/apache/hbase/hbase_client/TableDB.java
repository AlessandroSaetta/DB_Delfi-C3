//package org.xtce.test;
package org.apache.hbase.hbase_client;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.log4j.BasicConfigurator;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.TableName;

public class TableDB {
	
	public static void main(String[] args) throws IOException {
		  BasicConfigurator.configure();
		  
	      Configuration con = HBaseConfiguration.create();
	      
	      try (Connection connection = ConnectionFactory.createConnection(con); 
	             Admin admin = connection.getAdmin()) {
     

	      // Instantiating table descriptor class
	      HTableDescriptor tableDescriptor = new
	      HTableDescriptor(TableName.valueOf("DB_tel_GO"));

	      // Adding column families to table descriptor
//	      tableDescriptor.addFamily(new HColumnDescriptor("values"));
//	      tableDescriptor.addFamily(new HColumnDescriptor("tags"));
	      
	      HColumnDescriptor cd1 = new HColumnDescriptor ("values");
	      HColumnDescriptor cd2 = new HColumnDescriptor ("tags");
	      cd1.setMaxVersions(20);
	      cd2.setMaxVersions(20);
	      tableDescriptor.addFamily(cd1);
	      tableDescriptor.addFamily(cd2);
	      
	    
	      // Execute the table through admin
	      admin.createTable(tableDescriptor);
	   }
	      System.out.println(" Table created ");

	}

}
