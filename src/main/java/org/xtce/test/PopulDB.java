package org.xtce.test;

import java.io.File;
//import java.nio.ByteBuffer;
//import java.util.BitSet;
//import java.util.Iterator;
import java.util.List;
import org.xtce.toolkit.XTCEContainerContentEntry;
import org.xtce.toolkit.XTCEContainerContentModel;
import org.xtce.toolkit.XTCEContainerEntryValue;
import org.xtce.toolkit.XTCEDatabase;
import org.xtce.toolkit.XTCEDatabaseException;
import org.xtce.toolkit.XTCEParameter;
//import org.xtce.toolkit.XTCEFunctions;
//import org.xtce.toolkit.XTCETMContainer;
import org.xtce.toolkit.XTCETMStream;
import org.xtce.toolkit.XTCEValidRange;
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
//import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;



public class PopulDB {
//	static private int c = 0;
	 public static void main(String[] args) 
     {
 	

   	        String file = "Delfi-C3.xml";
   	        
   	        
   		     Configuration config = HBaseConfiguration.create();

   		      try (Connection conn = ConnectionFactory.createConnection(config);
   		        Table hTable = conn.getTable(TableName.valueOf("DB_raw"))) {
   		    	Table hTable1 = conn.getTable(TableName.valueOf("DB_tel_GO"));

   	            //System.out.println("Loading " + file + " database");

   	            XTCEDatabase db_ = new XTCEDatabase(new File(file), true, false, true);
   	            
//   	            List<String> warnings = db_.getDocumentWarnings();
//   	            Iterator<String> it = warnings.iterator();
//   	            while(it.hasNext())
//   	            {
//   	                System.err.println("ERROR: " + it.next());
//   	            }
   	        
   	            XTCETMStream stream = db_.getStream( "TLM" );
   	            
   	            // Instantiating the Scan class
   	            Scan scan = new Scan();
  
   	            // Scanning the required columns
   	            scan.addColumn(Bytes.toBytes("values"), Bytes.toBytes("Data"));
   	            scan.addColumn(Bytes.toBytes("values"), Bytes.toBytes("TS"));

   	            // Getting the scan result
   	            ResultScanner scanner = hTable.getScanner(scan);
   	            
//   	         int count = 0;
//   	        for (Result result : scanner) {
//   	            System.out.println("result = " + result);
//   	            count++;
//   	        }
//   	        System.out.printf("Scanned %d results\n", count);

                for (Result r : scanner) {
   	         
//   	           
   	            //scanner from timestamp to timestamp
//  	            for (int l=0; l<250; l++) {
//   	    	        
//    	           	// Instantiating Get class
//   	    	         Get g = new Get(Bytes.toBytes(l));
//
//   	    	         // Reading the data
//   	    	         Result result = hTable.get(g);
//
   	    	         byte[] tel = r.getValue(Bytes.toBytes("values"),Bytes.toBytes("Data"));
   	    	         byte[] ts  = r.getValue(Bytes.toBytes("values"),Bytes.toBytes("TS"));
   	    	         
   	    	         


   	                 processFrame(stream, tel, ts, hTable1); 
//   	             processFrame(stream, p);
//   	             processFrame(stream, hk);

   	                 
   	           }
                 
   	        } catch (XTCEDatabaseException ex)
   	        {
   	            ex.printStackTrace();
   	        } catch (Exception ex) {
   	            ex.printStackTrace();
   	        }
   	    }
        
   	    static void processFrame(XTCETMStream stream, byte[] data, byte[] ts, Table hTable) throws XTCEDatabaseException, Exception
   	    {
//   	    	int hash = data.hashCode();
  		    	     
  		               {
   	    	
   	    	XTCEContainerContentModel model = stream.processStream( data );
   	 
   	        List<XTCEContainerContentEntry> entries = model.getContentList();
            int paramCounter = 0;
   	        for (XTCEContainerContentEntry entry : entries) {
   	        	 
   	        	
//   	        System.out.print(entry.getName());
                
   	            XTCEContainerEntryValue val = entry.getValue();
   	           
   	            if (val == null)
   	            {
//   	                System.out.println(); 
   	            } else
   	            {

   	               	  Put p = new Put(Bytes.add(ts, Bytes.toBytes(paramCounter))); 
			              p.addColumn(Bytes.toBytes("values"),Bytes.toBytes("eng_value"),
			            		  Bytes.toBytes(val.getCalibratedValue()));
			              p.addColumn(Bytes.toBytes("tags"),Bytes.toBytes("parameter_name"),
			    	    		  Bytes.toBytes(entry.getName()));
			              p.addColumn(Bytes.toBytes("tags"),Bytes.toBytes("Unit"),
			    	    		  Bytes.toBytes(entry.getParameter().getUnits()));			             
			    	      p.addColumn(Bytes.toBytes("tags"),Bytes.toBytes("Sat"),
			    	    		  Bytes.toBytes("Delfi-C3")); 
			    	      p.addColumn(Bytes.toBytes("tags"),Bytes.toBytes("Timestamp"),
			    	    		  ts); 
			    	      if (isWithinValidRange(entry))
			    	      {
			    	    	  p.addColumn(Bytes.toBytes("tags"),Bytes.toBytes("validity"),
			    	    		  Bytes.toBytes(1));		    	    	  
			    	      }
			    	      else  
			    	      {
			    	    	  p.addColumn(Bytes.toBytes("tags"),Bytes.toBytes("validity"),
				    	    	  Bytes.toBytes(0));
//			    	    	  System.out.println("invalid data: " + entry.getName());
 			    	      }
			    	      
	    	    	      hTable.put(p);
//	    	    	    byte[] a = entry.getValue(Bytes.add(ts, Bytes.toBytes(paramCounter))); 
//	    	    	    byte[] b = entry.getValue(Bytes.add(Bytes.toBytes(paramCounter), ts));
	    	    	      
//	    	    	   System.out.print(Bytes.add(ts, Bytes.toBytes(paramCounter)));
//	    	    	   System.out.println("|" + Bytes.add(Bytes.toBytes(paramCounter), ts));
	    	    	      
//   	            	  c++;
//   	            	  System.out.println(": " + val.getCalibratedValue() + " "
//   	                        + entry.getParameter().getUnits() + " ("
//   	                        + val.getRawValueHex()+ ")");
	    	    	      
//	    	    	      if (!isWithinValidRange(entry))
//	    	    	      {
//	    	    	    	  System.out.println(" INVALID!");
//	    	    	      }
//	    	    	      else
//	    	    	      {
//	    	    	    	  System.out.println();
//                            
//	   	                 for(int i = 0; i < ts.length; i++) 
//		   		   	          System.out.println(String.format("%02X ", ts[i]));
//		   		   	          System.out.println();
////                            
		   	                 
//	    	    	      }
			    	      
   	              }
   	               paramCounter++;
   	             
   	           }
   	            //for insert in openTSDB
   	           // double num = Double.valueOf(val.getCalibratedValue());
   	        
  	       } 	                 
   	           	        
//   	        List<String> warnings = model.getWarnings();
//   	        Iterator<String> it = warnings.iterator();
//   	        while(it.hasNext())
//   	        {
//   	            System.err.println("WARNING: " + it.next());
//   	        }
  		               
//  		      System.out.println("data inserted!!!"); 
   	    }
      
   	    
   	 static private boolean isWithinValidRange(XTCEContainerContentEntry entry)
     {
         XTCEParameter param = entry.getParameter();
         XTCEValidRange range = param.getValidRange();
         if (!range.isValidRangeApplied()) {
             return true;
         } else {
             String valLow =  range.isLowValueCalibrated() ? 
                     entry.getValue().getCalibratedValue() : 
                     entry.getValue().getUncalibratedValue();

             if (range.isLowValueInclusive()) {
                 if (Double.parseDouble(valLow) < Double.parseDouble(range.getLowValue())) {
                     return false;
                 }
             } else {
                 if (Double.parseDouble(valLow) <= Double.parseDouble(range.getLowValue())) {
                     return false;
                 }
             }
             
             String valHigh =  range.isHighValueCalibrated() ? 
                     entry.getValue().getCalibratedValue() : 
                     entry.getValue().getUncalibratedValue();
             
             if (range.isHighValueInclusive()) {
                 if (Double.parseDouble(valHigh) > Double.parseDouble(range.getHighValue())) {
                     return false;
                 }
             } else {
                 if (Double.parseDouble(valHigh) >= Double.parseDouble(range.getHighValue())) {
                     return false;
                 }
             }
         }
         return true;
     }
 		      
    }    


