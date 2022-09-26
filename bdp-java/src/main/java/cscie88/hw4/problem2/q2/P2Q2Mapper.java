package cscie88.hw4.problem2.q2;

import java.io.IOException;
import java.time.format.DateTimeFormatter;

import cscie88.week2.LogLine;
import cscie88.week2.LogLineParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This is a Map part of a MR job that counts the number of unique urls per hour
 *
 * P2Q2Mapper converts a line in the input file
 * to a <key,value> pair where key is the date-hour in the line
 * plus a URL and the value the user for that request.
 * 
 * Input key is an Object.  It is not used in this mapper, so the type is kept
 * as Object so that it can accept any type. Input value is a Text that encloses an
 * entire line from the input file.  Output key is a Text that
 * encapsulates the date-hour-url string, and Output value is
 * a Text containing the user ID.
 */
public class P2Q2Mapper extends Mapper<Object, Text, Text, Text> {
	private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("uuuu-MM-dd:HH");
	private Text dateHourURLKey = new Text();
	private Text UserValue = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		LogLine parsedLogLine = LogLineParser.parseLine(value.toString());
		String dateHrStr = parsedLogLine.getEventDateTime().format(formatter);
		dateHourURLKey.set(dateHrStr + " - " + parsedLogLine.getUrl());

		UserValue.set(parsedLogLine.getUserId());
		context.write(dateHourURLKey, UserValue);
	}
}
