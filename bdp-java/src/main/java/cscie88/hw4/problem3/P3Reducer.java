package cscie88.hw4.problem3;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This is the Reduce part of a MR job that counts the number of unique users
 * per URL per hour
 *
 * P3Reducer takes as an input a <key,value[]> entry
 * where the key is a date-hour + URL string and value[] is a list of
 * URLs for that date-hour.
 *
 * The reducer iterates over the list of URL's and add them to a Set, thus
 * making them unique.
 * The result is the cardinality of the Set.
 */
public class P3Reducer extends Reducer<Text, Text, Text, IntWritable> {
	private IntWritable result = new IntWritable();

	public void reduce(Text dateHrKey, Iterable<Text> urls, Context context) throws IOException, InterruptedException {
		Set<String> uniqueURLs = new HashSet<String>();

		for (Text url : urls) {
			uniqueURLs.add(url.toString());
		}

		result.set(uniqueURLs.size());
		context.write(dateHrKey, result);
	}
}
