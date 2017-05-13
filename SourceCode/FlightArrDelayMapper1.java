

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightArrDelayMapper1 extends Mapper<LongWritable, Text, Text, LongWritable> {

	public void map(LongWritable lineOffSet, Text record, Context context) throws IOException, InterruptedException {
		String[] strArr = record.toString().split(",");
		// do not process first record as they are column headers
		if (strArr[0] != null && !strArr[0].equals("Year")) {
			String uniqueCarrier = strArr[8];
			// String flightNumber = strArr[9];
			String arrDelay = strArr[14];

			if (uniqueCarrier != null && arrDelay != null && !uniqueCarrier.trim().equals("")
					&& !arrDelay.trim().equals("")) {
				// if (uniqueCarrier != null && flightNumber != null && arrDelay
				// != null && !uniqueCarrier.trim().equals("")
				// && !flightNumber.trim().equals("") &&
				// !arrDelay.trim().equals("")) {
				try {
					// String airlineCode = uniqueCarrier + flightNumber;
					Integer arrDelayInt = Integer.parseInt(arrDelay);
					context.write(new Text("a-" + uniqueCarrier), new LongWritable(Integer.parseInt("1")));
					// if arrDelayInt > 10, we will count that flight being late
					if (arrDelayInt > 10) {
						context.write(new Text("b-" + uniqueCarrier), new LongWritable(Integer.parseInt("1")));
					}
				} catch (Exception e) {
					// e.printStackTrace();
					// no action if data is not correct. just ignore it
				}
			}
		}
	}
}
