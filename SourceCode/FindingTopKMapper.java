

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FindingTopKMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	public void map(LongWritable lineOffSet, Text record, Context context)
			throws IOException, InterruptedException {

		String[] strArr = record.toString().split("\t");
		context.write(new DoubleWritable(Double.parseDouble(strArr[1].trim())), new Text(strArr[0]));

	}
}
