

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightArrDelayReducer1<KEY> extends Reducer<Text, LongWritable, Text, DoubleWritable> {

	// private LongWritable result = new LongWritable();
	double max1 = 0L;
	double max2 = 0L;
	double max3 = 0L;
	double min1 = Double.MAX_VALUE;
	double min2 = Double.MAX_VALUE;
	double min3 = Double.MAX_VALUE;
	Text max1AirlineCode = new Text();
	Text max2AirlineCode = new Text();
	Text max3AirlineCode = new Text();
	Text min1AirlineCode = new Text();
	Text min2AirlineCode = new Text();
	Text min3AirlineCode = new Text();

	Map<String, Long> totalSumMap = new HashMap<String, Long>();

	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		try {
			String keyStr = key.toString();
			System.out.println(keyStr);
			if (keyStr.contains("a-")) {
				long totalSum = 0;

				for (LongWritable val : values) {
					totalSum += val.get();
				}
				String airlineCode = keyStr.replace("a-", "");
				totalSumMap.put(airlineCode, totalSum);
			} else {

				long sum = 0;
				String airlineCode = keyStr.replace("b-", "");
				long totalSum = totalSumMap.get(airlineCode);

				for (LongWritable val : values) {
					sum += val.get();
				}

				double delayProb = sum / (double) totalSum;

				if (max1 < delayProb) {
					max3 = max2;
					max3AirlineCode.set(max2AirlineCode.toString());
					max2 = max1;
					max2AirlineCode.set(max1AirlineCode.toString());
					max1 = delayProb;
					max1AirlineCode.set(airlineCode);
				} else if (max2 < delayProb) {
					max3 = max2;
					max3AirlineCode.set(max2AirlineCode.toString());
					max2 = delayProb;
					max2AirlineCode.set(airlineCode);
				} else if (max3 < delayProb) {
					max3 = delayProb;
					max3AirlineCode.set(airlineCode);
				}

				if (min1 > delayProb) {
					min3 = min2;
					min3AirlineCode.set(min2AirlineCode.toString());
					min2 = min1;
					min2AirlineCode.set(min1AirlineCode.toString());
					min1 = delayProb;
					min1AirlineCode.set(airlineCode);
				} else if (min2 > delayProb) {
					min3 = min2;
					min3AirlineCode.set(min2AirlineCode.toString());
					min2 = delayProb;
					min2AirlineCode.set(airlineCode);
				} else if (min3 > delayProb) {
					min3 = delayProb;
					min3AirlineCode.set(airlineCode);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		// result.set(sum);
		// uncomment to print count of all cancellation codes
		// context.write(key, result);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		context.write(new Text("lowest probability for being on schedule"), null);
		context.write(max1AirlineCode, new DoubleWritable(max1));
		context.write(max2AirlineCode, new DoubleWritable(max2));
		context.write(max3AirlineCode, new DoubleWritable(max3));
		context.write(new Text("highest probability for being on schedule"), null);
		context.write(min1AirlineCode, new DoubleWritable(min1));
		context.write(min2AirlineCode, new DoubleWritable(min2));
		context.write(min3AirlineCode, new DoubleWritable(min3));
	}
}
