package joinset;


import java.io.IOException;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public   class JointwoMapper extends Mapper < LongWritable , Text, Text, Text > {
       	
        	
public void map(LongWritable key, Text value,  Context context)
        throws IOException, InterruptedException {
 
String cellno,statuscode,tag="BBB~";
	
	String[] parts = value.toString().split("[,]");
    cellno=parts[0];
    statuscode=parts[1];
    		
        context.write( new Text(cellno), new Text(tag + statuscode));
    } 
}









