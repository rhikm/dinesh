package joinset;


//import java.io.File;

//import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
//import org.apache.hadoop.mapred.lib.MultipleInputs;
//import org.apache.hadoop.mapred.JobInProgress.Counter;
//import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
//import org.apache.hadoop.mapred.jobcontrol.Job;
//import org.apache.hadoop.log.metrics.EventCounter;

//import com.dinesh.test.UniqueListeners.Counter;

public  class joindriver {
	
	public static enum Counter {
		INVALID_RECORD_COUNT
	}
public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 4) {
            System.err.println("Usage: joindriver  < in > <in> <in> < out >");
            System.exit(2);
        }
        String[] files=new GenericOptionsParser(conf,args).getRemainingArgs();
        Path p3=new Path(files[2]);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(p3)){
			fs.delete(p3, true);
		}
        Job job = new Job(conf, "Join two file");
        job.setJarByClass(joindriver.class);
       // job.setMapperClass(JoinoneMapper.class);
        //job.setNumReduceTasks(0);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
       // conf.addDefaultResource(new File("/resource.xml").toURI();
    	
      //  DistributedCache.addCacheArchive(new URI(new Path(args[3])),conf);
    //	MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, UserFileMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[0]),  TextInputFormat.class, JoinoneMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),  TextInputFormat.class, JointwoMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        DistributedCache.addCacheFile(new Path(args[3]).toUri(), job.getConfiguration());
        
        
        int exitcode =job.waitForCompletion(true) ? 0 : 1;
        
       // System.exit(job.waitForCompletion(true) ? 0 : 1);
        Counters counters = job.getCounters();
        System.out.println(exitcode+"No. of Invalid Records :" + counters.findCounter(Counter.INVALID_RECORD_COUNT).getValue());
       // org.apache.hadoop.mapreduce.Counters counters = job.getCounters();
       // System.out.println("No. of Invalid Records :"
        //        + counters.findCounter(Counter.INVALID_RECORD_COUNT)
         //               .getValue()); 
 
        
    }

}

