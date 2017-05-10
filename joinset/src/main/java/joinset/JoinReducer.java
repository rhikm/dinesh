package joinset;


import java.io.BufferedReader;
//import java.io.File;
import java.io.FileReader;
import java.io.IOException;
//import java.net.URI;
import java.util.HashMap;
//import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
//import java.util.Set;


//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.Counters.Counter;
//import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

//import com.dinesh.test.UniqueListenersMapper.Counter;

public  class JoinReducer extends  Reducer< Text , Text, Text, Text> {
    String custname,deliveryReport;
   // private MapFile.Reader deptMapReader = null;
    private static Map<String,String> DeliveryCodesMap= new HashMap<String,String>();
    //private Set stopWords = new HashSet();
    public enum Counter {
    	INVALID_RECORD_COUNT
    }
    
   // loadDeliveryStatusCodes();
 // Get side data from the distributed cache
 		
    
    protected void setup(Context context) throws IOException, InterruptedException {
    	
    	        try {
    	            Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
    	            if(stopWordsFiles != null && stopWordsFiles.length > 0) {
    	                for(Path stopWordFile : stopWordsFiles) {
    	                    readFile(stopWordFile);
    	                   // System.out.println("HI");
    	                }
    	            }
    	        } catch(IOException ex ) {
    	            System.err.println("Exception in mapper setup: " + ex.getMessage());}
    	        }
    	    

private void readFile(Path filePath) {
      try{
    	  
          @SuppressWarnings("resource")
		BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
          String stopWord = null;
          while ((stopWord=bufferedReader.readLine() ) != null)
          {
                String splitarray[] = stopWord.split(",");
                //parse record and load into HahMap
                DeliveryCodesMap.put(splitarray[0].trim(), splitarray[1].trim());
               
          }
      } catch(IOException ex) {
          System.err.println("Exception while reading stop words file: " + ex.getMessage());
      }
  }



    public void reduce(Text key, Iterable <Text> values, Context context) throws IOException,InterruptedException
    {
    	for (Text value : values) {
    	//context.getCounter(Counter.INVALID_RECORD_COUNT).increment(5L);
    	String parts[] = value.toString().split("~");
        //String parts[] = currValue.split("~");
    	//String[] parts = values.toString().split("[~]");
    	if(parts[0].equals("AAA"))
    	{
        custname= parts[1].trim();
    	}
    	else if (parts[0].equals("BBB") /*&& ((parts[1].trim())==DeliveryCodesMap.get(parts[0].trim()))*/)
    		
    	{
    		    		
    		deliveryReport =  DeliveryCodesMap.get(parts[1].trim());
    		//deliveryReport = parts[1].trim();
    	}
    		
    	 if(custname!=null && deliveryReport!=null)
         {
                context.write(new Text(custname), new Text(deliveryReport));
         }
         else if(custname==null)
        	 context.write(new Text("customerName"), new Text(deliveryReport));
    	 
         else if(deliveryReport==null)
        	 context.write(new Text(custname), new Text("deliveryReport"));
        
    	 
         }   
     }
    
/*
private void loadDeliveryStatusCodes()
{
   String strRead;
   try {
          //read file from Distributed Cache
                 BufferedReader reader = new BufferedReader(new FileReader("DeliveryStatusCodes.txt"));
                 while ((strRead=reader.readLine() ) != null)
                 {
                       String splitarray[] = strRead.split(",");
                       //parse record and load into HahMap
                       DeliveryCodesMap.put(splitarray[0].trim(), splitarray[1].trim());
                      
                 }
          }
          catch (FileNotFoundException e) {
          e.printStackTrace();
          }catch( IOException e ) {
                   e.printStackTrace();
            }
         
   } */
}
            
