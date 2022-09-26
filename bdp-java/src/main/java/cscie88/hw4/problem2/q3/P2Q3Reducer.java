package cscie88.hw4.problem2.q3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This is the Reduce part of a MR job that counts the number of unique users per URL per hour
 *
 * P2Q3Reducer takes as an input a <key,value[]> entry
 * where the key is a date-hour + URL string and value[] is a list of
 * number of clicks that URL for that date-hour.
 */
public class P2Q3Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();

	public void reduce(Text dateHrURLKey, Iterable<IntWritable> eventCounts, Context context) throws IOException, InterruptedException {
		int totalEvents = 0;

		for(IntWritable eventCount:eventCounts) {
			totalEvents += eventCount.get();
		}

		result.set(totalEvents);
		context.write(dateHrURLKey, result);
	}
}
