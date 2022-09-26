package cscie88.hw4.problem2.q2;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This is the Reduce part of a MR job that counts the number of unique users per URL per hour
 *
 * P2Q2Reducer takes as an input a <key,value[]> entry
 * where the key is a date-hour + URL string and value[] is a list of
 * users per that URL for that date-hour.
 * 
 * The reducer iterates over the list of users and add them to a Set, thus making them unique.
 * The result is the cardinality of the Set.
 */
public class P2Q2Reducer extends Reducer<Text, Text, Text, IntWritable> {
	private IntWritable result = new IntWritable();

	public void reduce(Text dateHrURLKey, Iterable<Text> urls, Context context) throws IOException, InterruptedException {
		Set<String> uniqueUsers = new HashSet<String>();

		for (Text url: urls) {
			uniqueUsers.add(url.toString());
		}

		result.set(uniqueUsers.size());
		context.write(dateHrURLKey, result);
	}
}
