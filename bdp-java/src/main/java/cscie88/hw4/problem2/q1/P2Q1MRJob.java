package cscie88.hw4.problem2.q1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Main MR runner/job that creates, configures and runs a MR job that counts the number of url's
 * per date/hour
 */
public class P2Q1MRJob {

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Usage: java P2Q1MRJob <input_dir> <output_dir>");
			System.exit(-1);
		}
		// create a new MR job
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Count unique URL's per hour");
	    
	    //This class is used as reference point to locate the jar file
	    job.setJarByClass(P2Q1MRJob.class);
	    
	    //set the Mapper class for the job
	    job.setMapperClass(P2Q1Mapper.class);
	    
	    //set the Reducer for the job
	    job.setReducerClass(P2Q1Reducer.class);

	    //set output key and value classes/types for the mapper.
		job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);

	    //set output key and value classes/types for the reducer.
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

		// set input and output formats - even though the ones we use are the default ones
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

		//Input path is the first argument given on the command line
	    //when the job is kicked off.  And Output path is the second argument.
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    //wait for the job to complete
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
