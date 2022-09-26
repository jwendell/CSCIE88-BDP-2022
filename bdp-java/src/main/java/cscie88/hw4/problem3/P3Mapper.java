package cscie88.hw4.problem3;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import cscie88.week2.LogLine;
import cscie88.week2.LogLineParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This is a Map part of a MR job that counts the number of unique urls per
 * country hour
 *
 * P3Mapper converts a line in the input file
 * to a <key,value> pair where key is the date-hour in the line
 * plus the country code and the value is a URL.
 * 
 * Input key is an Object. It is not used in this mapper, so the type is kept
 * as Object so that it can accept any type. Input value is a Text that encloses
 * an
 * entire line from the input file. Output key is a Text that
 * encapsulates the date-hour-country string, and Output value is
 * the URL.
 */
public class P3Mapper extends Mapper<Object, Text, Text, Text> {
	private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("uuuu-MM-dd:HH");
	private Text dateHourURLKey = new Text();
	private Text URLValue = new Text();

	private ZonedDateTime initialDateTimeHour;
	private ZonedDateTime finalDateTimeHour;
	private boolean gotDatesFromConfiguration;

	private void getDatesFromConfiguration(Context context) {
		String initialDate = context.getConfiguration().get("initialDate");
		String finalDate = context.getConfiguration().get("finalDate");
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("uuuu-MM-dd:HH");

		LocalDateTime ldt = LocalDateTime.parse(initialDate, formatter);
		initialDateTimeHour = ZonedDateTime.of(ldt, ZoneId.of(ZoneOffset.UTC.getId()));

		ldt = LocalDateTime.parse(finalDate, formatter);
		finalDateTimeHour = ZonedDateTime.of(ldt, ZoneId.of(ZoneOffset.UTC.getId()));

		gotDatesFromConfiguration = true;
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		LogLine parsedLogLine = LogLineParser.parseLine(value.toString());
		String dateHrStr = parsedLogLine.getEventDateTime().format(formatter);
		dateHourURLKey.set(dateHrStr + " - " + parsedLogLine.getCountry());

		// Small optimization, to only retrieve the dates on the first run. Ideally
		// should be passed via constructor
		if (!gotDatesFromConfiguration) {
			getDatesFromConfiguration(context);
		}

		ZonedDateTime current = parsedLogLine.getEventDateTime().withMinute(0).withSecond(0).withNano(0);

		// Filter out dates that are not in the range specified as argument to the job:
		// Only produce a result if initialDate >= current <= finalDate
		if ((current.isEqual(initialDateTimeHour) || current.isAfter(initialDateTimeHour))
				&& (current.isEqual(finalDateTimeHour) || current.isBefore(finalDateTimeHour))) {
			URLValue.set(parsedLogLine.getUrl());
			context.write(dateHourURLKey, URLValue);
		}
	}
}
