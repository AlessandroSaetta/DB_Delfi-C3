package org.xtce.test;
import java.awt.Color;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.awt.BasicStroke; 
import org.jfree.chart.ChartPanel; 
import org.jfree.chart.JFreeChart; 
import org.jfree.data.xy.XYDataset; 
import org.jfree.data.xy.XYSeries; 
import org.jfree.ui.ApplicationFrame; 
import org.jfree.ui.RefineryUtilities; 
import org.jfree.chart.plot.XYPlot;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.jfree.chart.ChartFactory; 
import org.jfree.chart.plot.PlotOrientation; 
import org.jfree.data.xy.XYSeriesCollection; 
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import java.io.File;
import java.io.IOException;
//import java.nio.charset.Charset;
//import java.nio.charset.StandardCharsets;
//import java.util.BitSet;
//import java.util.Iterator;
//import java.util.List;
//import org.xtce.toolkit.XTCEContainerContentEntry;
//import org.xtce.toolkit.XTCEContainerContentModel;
//import org.xtce.toolkit.XTCEContainerEntryValue;
//import org.xtce.toolkit.XTCEDatabase;
//import org.xtce.toolkit.XTCEDatabaseException;
//import org.xtce.toolkit.XTCEParameter;
//import org.xtce.toolkit.XTCEFunctions;
//import org.xtce.toolkit.XTCETMContainer;
//import org.xtce.toolkit.XTCETMStream;
//import org.xtce.toolkit.XTCEValidRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.ResultScanner;
//import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
//import org.apache.hadoop.hbase.client.Get;
//import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;


public class XYLineChart_AWT extends ApplicationFrame {
	

	   public XYLineChart_AWT( String applicationTitle, String chartTitle ) throws IOException {
	      super(applicationTitle);
	      JFreeChart xylineChart = ChartFactory.createXYLineChart(
	         chartTitle ,
	         "Category" ,
	         "Values" ,
	         createDataset() ,
	         PlotOrientation.VERTICAL ,
	         true , true , false);
	         
	      ChartPanel chartPanel = new ChartPanel( xylineChart );
	      chartPanel.setPreferredSize( new java.awt.Dimension( 560 , 367 ) );
	      final XYPlot plot = xylineChart.getXYPlot( );
	      
	      XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer( );
	      renderer.setSeriesPaint( 0 , Color.YELLOW );
	      renderer.setSeriesPaint( 1 , Color.GREEN );

	      renderer.setSeriesStroke( 0 , new BasicStroke( 4.0f ) );
	      renderer.setSeriesStroke( 1 , new BasicStroke( 3.0f ) );

	      plot.setRenderer( renderer ); 
	      setContentPane( chartPanel ); 
	   }
	   
	   
	   private XYSeriesCollection createDataset( ) throws IOException {
		      			
			      
			    Configuration config = HBaseConfiguration.create();

       	        try (Connection conn = ConnectionFactory.createConnection(config);
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
			     int count = 0;  	
	   	         for (Result r : scanner) {
	   	         	 
	   	        	  byte [] bnam = r.getValue(Bytes.toBytes("tags"),Bytes.toBytes("parameter_name"));
		   	          String name = Bytes.toString(bnam);
//		   	          System.out.println(name + " | ");
		              
//	              
		   	          byte [] bval = r.getValue(Bytes.toBytes("values"),Bytes.toBytes("eng_value"));
		   	          String sval ;
		   	          try {
		   	          sval = Bytes.toString(bval);
//		   	          System.out.print(sval + " | ");
	//	   	          double value = 1.0;
		   	          double val = Double.parseDouble(sval); 
//		   	          System.out.print(val + " | ");
		   	          } catch (NumberFormatException e)
		   	          {
//		   	        	 System.out.print(bval + " | ");
		   	          }
	   	          	   	   	   	            	  

	//	   	          System.out.println(r.raw());
		   	          
		   	          byte [] bkey = r.getValue(Bytes.toBytes("tags"),Bytes.toBytes("Timestamp"));
		   	         
//		   	          for(int i = 0; i < bkey.length; i++)
//		   	          System.out.println(String.format("%02X ", bkey[i]));
//	   	              System.out.println();
//		   	          long ts = Bytes.toLong(bkey);
//		   	          Calendar cal = Calendar.getInstance();
//		   	          cal.setTimeZone(TimeZone.getTimeZone("UTC"));
//		   	          cal.setTimeInMillis(ts);
//		   	          SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
//		   	     	  String date = sdf.format(cal.getTime());
	   		
//		   	          System.out.println(ts + " | " + date + " | ");
//		   	          System.out.println();
		   	          String skey = Bytes.toString(bkey);
	//	   	          System.out.print(skey);
	//	   	          System.out.println();
		   	          double TS = Double.parseDouble(skey);
	//	   	          System.out.println(key);	   	          
	//	   	          double TS = 0.0;
		   	          //result.raw()[0].getTimestamp();   	          
		   	          count++;
		   	          } 
	//	   	      System.out.printf("Scanned %d results\n", count);

	   	             	          
	   	          
	              final XYSeries param1 = new XYSeries( "bus_V_dep" );        
	              param1.add(TS,val);	              
	                                   
 
//	              final XYSeries param2 = new XYSeries( "TSFC_IV_ZpXp" );    
//	              if (name == "TSFC_IV_ZpXp") {
//		          param2.add(TS,value);	}  
	                        

			      final XYSeriesCollection dataset = new XYSeriesCollection( ); 
	    	      dataset.addSeries(param1); 
//	    	      dataset.addSeries(param2);  
	    	      return dataset;
	     

          
			   }
			return createDataset();    
	     }   
  


	   public static void main( String[ ] args ) throws IOException {
	      XYLineChart_AWT chart = new XYLineChart_AWT("Parameters Plot",
	         "bus_V_dep");
	      chart.pack( );          
	      RefineryUtilities.centerFrameOnScreen( chart );          
	      chart.setVisible( true ); //to view chart set to true
	   }

}
