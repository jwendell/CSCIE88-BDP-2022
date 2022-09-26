package cscie88.hw4.problem2.q3;

import java.io.IOException;
import java.time.format.DateTimeFormatter;

import cscie88.week2.LogLine;
import cscie88.week2.LogLineParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This is a Map part of a MR job that counts the number of unique urls per hour
 *
 * P2Q3Mapper converts a line in the input file
 * to a <key,value> pair where key is the date-hour in the line
 * plus a URL and the value is 1.
 * 
 * Input key is an Object.  It is not used in this mapper, so the type is kept
 * as Object so that it can accept any type. Input value is a Text that encloses an
 * entire line from the input file.  Output key is a Text that
 * encapsulates the date-hour-url string, and Output value is
 * the number 1.
 */
public class P2Q3Mapper extends Mapper<Object, Text, Text, IntWritable> {
	private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("uuuu-MM-dd:HH");
	private Text dateHourURLKey = new Text();
	private final IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		LogLine parsedLogLine = LogLineParser.parseLine(value.toString());
		String dateHrStr = parsedLogLine.getEventDateTime().format(formatter);
		dateHourURLKey.set(dateHrStr + " - " + parsedLogLine.getUrl());

		context.write(dateHourURLKey, one);
	}
}
