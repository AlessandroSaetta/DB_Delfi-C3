package org.xtce.test;

import java.io.File;
//import java.util.BitSet;
//import java.util.Iterator;
import java.util.List;
import org.xtce.toolkit.XTCEContainerContentEntry;
import org.xtce.toolkit.XTCEContainerContentModel;
import org.xtce.toolkit.XTCEContainerEntryValue;
import org.xtce.toolkit.XTCEDatabase;
import org.xtce.toolkit.XTCEDatabaseException;
//import org.xtce.toolkit.XTCEFunctions;
//import org.xtce.toolkit.XTCETMContainer;
import org.xtce.toolkit.XTCETMStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
//import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.ResultScanner;
//import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;


public class PopulateDB {
	
	
	  public static void main(String[] args) 
      {

    	
    		   

    	        String file = "Delfi-C3.xml";
    	        
    	        
    		     Configuration config = HBaseConfiguration.create();

    		      try (Connection conn = ConnectionFactory.createConnection(config);
    		    	      Table hTable = conn.getTable(TableName.valueOf("Telemetry"))) {
   	   
    	            //System.out.println("Loading " + file + " database");

    	            XTCEDatabase db_ = new XTCEDatabase(new File(file), true, false, true);
    	           
//    	            List<String> warnings = db_.getDocumentWarnings();
//    	            Iterator<String> it = warnings.iterator();
//    	            while(it.hasNext())
//    	            {
//    	                System.err.println("ERROR: " + it.next());
//    	            }

    	            XTCETMStream stream = db_.getStream( "TLM" );
    	           
    	            for (int l=0; l<6; l++) {
    	    	        
    	            	System.out.println("debuggg");
    	            	// Instantiating Get class
    	    	         Get g = new Get(Bytes.toBytes(l));

    	    	         // Reading the data
    	    	         Result result = hTable.get(g);

    	    	         byte [] tel = result.getValue(Bytes.toBytes("values"),Bytes.toBytes("Data"));

    	             processFrame(stream, tel); 
    	           //processFrame(stream, p);
    	           //processFrame(stream, hk);
    	             
    	           }
                  
    	        } catch (XTCEDatabaseException ex)
    	        {
    	            ex.printStackTrace();
    	        } catch (Exception ex) {
    	            ex.printStackTrace();
    	        }
    	    }
         
    	    static void processFrame(XTCETMStream stream, byte[] data) throws XTCEDatabaseException, Exception
    	    {
    	    	 Configuration config = HBaseConfiguration.create();

   		         try (Connection conn = ConnectionFactory.createConnection(config);
   		    	     
   		              Table hTable = conn.getTable(TableName.valueOf("DB"))) {
    	    	
    	    	XTCEContainerContentModel model = stream.processStream( data );
    	 
    	        List<XTCEContainerContentEntry> entries = model.getContentList();
               
    	        
    	        int k = 0;
    	        for (XTCEContainerContentEntry entry : entries) {
//    	        	for (int k=0; k<4; k++) { 
    	        	
    	        //    System.out.print(entry.getName());
                    
    	            XTCEContainerEntryValue val = entry.getValue();

    	            if (val == null)
    	            {
    	               // System.out.println();
    	            } else
    	            {
    	            	  Put p = new Put(Bytes.toBytes(k)); 
			              p.addColumn(Bytes.toBytes("values"),Bytes.toBytes("eng_value"),
			            		  Bytes.toBytes(val.getCalibratedValue()));
//			              p.addColumn(Bytes.toBytes("tags"),Bytes.toBytes("parameter_name"),
//			    	    		  Bytes.toBytes(entry.getName()));
//			              p.addColumn(Bytes.toBytes("tags"),Bytes.toBytes("Unit"),
//			    	    		  Bytes.toBytes(entry.getParameter().getUnits()));			             
//			    	      p.addColumn(Bytes.toBytes("tags"),Bytes.toBytes("Sat"),
//			    	    		  Bytes.toBytes("Delfi-C3"));
			    	      hTable.put(p);
			    	      k++;
//    	            	
//    	            	System.out.println(": " + val.getCalibratedValue() + " "
//    	                        + entry.getParameter().getUnits() + " ("
//    	                        + val.getRawValueHex()+ ")");
    	              }
    	            }
    	          //  }
    	            //for insert in openTSDB
    	           // double num = Double.valueOf(val.getCalibratedValue());
    	        }
    	        
    	        
    	        
//    	        List<String> warnings = model.getWarnings();
//    	        Iterator<String> it = warnings.iterator();
//    	        while(it.hasNext())
//    	        {
//    	            System.err.println("WARNING: " + it.next());
//    	        }
   		      System.out.println("data inserted!!!"); 
    	    }
        
  		      
  	    }    

