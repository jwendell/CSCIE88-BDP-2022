package cscie88.hw4.problem3;

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
 * Main MR runner/job that creates, configures and runs a MR job that counts the number of
 * unique URLs per country per date/hour
 */
public class P3MRJob {

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.out.println("Usage: java P3MRJob <input_dir> <output_dir> <initial-date> <final-date>");
			System.exit(-1);
		}
		// create a new MR job
		Configuration conf = new Configuration();
		conf.set("initialDate", args[2]);
		conf.set("finalDate", args[3]);
	    Job job = Job.getInstance(conf, "Count of unique URLs by country by hour for a specified time range");

	    //This class is used as reference point to locate the jar file
	    job.setJarByClass(P3MRJob.class);

	    //set the Mapper class for the job
	    job.setMapperClass(P3Mapper.class);

	    //set the Reducer for the job
	    job.setReducerClass(P3Reducer.class);

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
