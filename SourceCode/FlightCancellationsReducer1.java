

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightCancellationsReducer1<KEY> extends Reducer<Text, LongWritable, Text, LongWritable> {

	private LongWritable result = new LongWritable();
	long max = 0L;
	Text maxCancellationCode = new Text();

	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		long sum = 0;
		for (LongWritable val : values) {
			sum += val.get();
		}

		if (sum >= max) {
			maxCancellationCode.set(key);
			max = sum;
		}

		result.set(sum);
		// uncomment to print count of all cancellation codes
		// context.write(key, result);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(maxCancellationCode, new LongWritable(max));
	}
}
