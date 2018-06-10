package org.xtce.test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;


public class FilterScan {

	public static void main(String[] args) throws IOException {
	    Configuration conf = HBaseConfiguration.create();

	    try (Connection conn = ConnectionFactory.createConnection(conf);
   		        Table hTable = conn.getTable(TableName.valueOf("DB_tel_GO"))) {

	    List<Filter> filters = new ArrayList<Filter>();

	    SingleColumnValueFilter colValFilter = new SingleColumnValueFilter(Bytes.toBytes("tags"), Bytes.toBytes("parameter_name")
	            , CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("bus_V_dep")));
	    colValFilter.setFilterIfMissing(false);
	    filters.add(colValFilter);   
	    
	  
	    FilterList fl = new FilterList( FilterList.Operator.MUST_PASS_ALL, filters);
        

	    Scan scan = new Scan();
	    scan.setFilter(fl);
	    scan.addColumn(Bytes.toBytes("values"), Bytes.toBytes("eng_value"));
	    scan.addColumn(Bytes.toBytes("tags"), Bytes.toBytes("parameter_name"));
	    scan.addColumn(Bytes.toBytes("tags"), Bytes.toBytes("Timestamp"));	   

	    ResultScanner scanner = hTable.getScanner(scan);

	    System.out.println("Scanning table... ");
	    int count = 0;
	    int count_PL = 0;
	    int count_HK = 0;
	    
	    for (Result r : scanner) {
	    	
	    	  byte [] bnam = r.getValue(Bytes.toBytes("tags"),Bytes.toBytes("parameter_name"));
  	          String name = Bytes.toString(bnam); 	          
 	         
  	          System.out.print(name + " |");
	    	
	    	  byte [] bval = r.getValue(Bytes.toBytes("values"),Bytes.toBytes("eng_value"));
  	          String sval ;
  	          
  	          try {
  	          sval = Bytes.toString(bval);
  	          System.out.print(sval + " | "); 
// 	          if (sval == "Payload")
//  	             {count_PL++;}
//  	          if (sval == "Housekeeping") 
// 	          else
//        	     {count_HK++;}
  	          double val = Double.parseDouble(sval); 
  	          System.out.print(val);
  	          } catch (NumberFormatException e)
  	          {
  	        	 System.out.print(bval + " | ");
  	          }  	
  	          count++;	          
  	 
   	          System.out.println();
   	         
	    } 
	    scanner.close();
//	    System.out.println("Number of PL frames : " + count_PL);
//	    System.out.println("Number of HK frames: " + count_HK);
	    System.out.println("Number of parameters: " + count);
	    System.out.println("Completed ");
	   }
	}
}
	
  
	
