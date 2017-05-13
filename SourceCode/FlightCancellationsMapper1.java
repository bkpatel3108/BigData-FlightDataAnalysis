

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightCancellationsMapper1 extends Mapper<LongWritable, Text, Text, LongWritable> {

	public void map(LongWritable lineOffSet, Text record, Context context) throws IOException, InterruptedException {
		String[] strArr = record.toString().split(",");
		// do not process first record as they are column headers
		if (strArr[0] != null && !strArr[0].equals("Year")) {
			String flightCancelledCode = strArr[22];
			if (flightCancelledCode != null && !flightCancelledCode.trim().equals("")) {
				context.write(new Text(flightCancelledCode), new LongWritable(Integer.parseInt("1")));
			}
		}
	}
}
