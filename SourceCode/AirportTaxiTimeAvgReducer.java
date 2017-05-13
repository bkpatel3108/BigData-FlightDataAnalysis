

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AirportTaxiTimeAvgReducer<KEY> extends Reducer<Text, LongWritable, Text, DoubleWritable> {

	private DoubleWritable result = new DoubleWritable();

	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		double sum = 0;
		long count = 0;
		for (LongWritable val : values) {
			sum += val.get();
			count++;
		}
		if (count > 0) {
			result.set(sum / (double) count);
			context.write(key, result);
		}
	}
}
