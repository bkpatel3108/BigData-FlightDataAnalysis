

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable.Comparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FlightDataAnalysisMain extends Configured implements Tool {

	// hadoop directory paths
	private static final String TEMP_OUTPUT_PATH1 = "/temp_output1";
	private static final String TEMP_OUTPUT_PATH2 = "/temp_output2";

	//private static final String TEMP_OUTPUT_PATH1 = "/Users/bhaumikpatel/njit/workspaces/big-data-learning/FlightDataAnalysis/src/main/java/temp_output1";
	//private static final String TEMP_OUTPUT_PATH2 = "/Users/bhaumikpatel/njit/workspaces/big-data-learning/FlightDataAnalysis/src/main/java/temp_output2";

	//
	public int run(String[] arg0) throws Exception {
		try {

			FileSystem fs = FileSystem.get(getConf());

			/*
			 * 
			 * Airport Cancellation reason/code
			 */
			Job job = Job.getInstance(getConf());
			job.setJarByClass(getClass());

			FileInputFormat.setInputPaths(job, new Path(arg0[0]));

			// FileSystem fs = FileSystem.get(getConf());
			if (fs.exists(new Path(arg0[1] + "_FlightCancellationMostCommonReason"))) {
				fs.delete(new Path(arg0[1] + "_FlightCancellationMostCommonReason"), true);
			}

			job.setMapperClass(FlightCancellationsMapper1.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(LongWritable.class);

			job.setReducerClass(FlightCancellationsReducer1.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);

			FileOutputFormat.setOutputPath(job, new Path(arg0[1] + " v"));
			// TODO Auto-generated method stub

			job.waitForCompletion(true);

			/*
			 *
			 * Airport Taxi In time first job
			 */

			Job job2 = Job.getInstance(getConf());
			job2.setJarByClass(getClass());

			job2.setMapperClass(AirportTaxiInTimeMapper.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(LongWritable.class);

			job2.setReducerClass(AirportTaxiTimeAvgReducer.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(DoubleWritable.class);

			FileInputFormat.addInputPath(job2, new Path(arg0[0]));
			if (fs.exists(new Path(TEMP_OUTPUT_PATH1))) {
				fs.delete(new Path(TEMP_OUTPUT_PATH1), true);
			}
			FileOutputFormat.setOutputPath(job2, new Path(TEMP_OUTPUT_PATH1));

			job2.waitForCompletion(true);

			/*
			 *
			 * Airport Taxi In time second job - Max
			 */
			Job job3 = Job.getInstance(getConf());
			job3.setJarByClass(getClass());

			job3.setMapperClass(FindingTopKMapper.class);
			job3.setMapOutputKeyClass(DoubleWritable.class);
			job3.setMapOutputValueClass(Text.class);

			job3.setNumReduceTasks(1);

			job3.setCombinerClass(FindingTop3Combiner.class);
			job3.setReducerClass(FindingTop3Reducer.class);
			job3.setSortComparatorClass(LongWritable.DecreasingComparator.class);

			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(DoubleWritable.class);

			FileInputFormat.addInputPath(job3, new Path(TEMP_OUTPUT_PATH1));
			if (fs.exists(new Path(arg0[1] + "_AirportTaxiInTime_Max"))) {
				fs.delete(new Path(arg0[1] + "_AirportTaxiInTime_Max"), true);
			}
			FileOutputFormat.setOutputPath(job3, new Path(arg0[1] + "_AirportTaxiInTime_Max"));

			job3.waitForCompletion(true);
			/*
			 *
			 * Airport Taxi In time third job - Min
			 */
			Job job4 = Job.getInstance(getConf());
			job4.setJarByClass(getClass());

			job4.setMapperClass(FindingTopKMapper.class);
			job4.setMapOutputKeyClass(DoubleWritable.class);
			job4.setMapOutputValueClass(Text.class);

			job4.setNumReduceTasks(1);

			job4.setCombinerClass(FindingTop3Combiner.class);
			job4.setReducerClass(FindingTop3Reducer.class);
			job4.setSortComparatorClass(DoubleWritable.Comparator.class);

			job4.setOutputKeyClass(Text.class);
			job4.setOutputValueClass(DoubleWritable.class);

			FileInputFormat.addInputPath(job4, new Path(TEMP_OUTPUT_PATH1));
			if (fs.exists(new Path(arg0[1] + "_AirportTaxiInTime_Min"))) {
				fs.delete(new Path(arg0[1] + "_AirportTaxiInTime_Min"), true);
			}
			FileOutputFormat.setOutputPath(job4, new Path(arg0[1] + "_AirportTaxiInTime_Min"));

			job4.waitForCompletion(true);

			/*
			 *
			 * Airport Taxi Out time first job
			 */

			Job job5 = Job.getInstance(getConf());
			job5.setJarByClass(getClass());

			job5.setMapperClass(AirportTaxiOutTimeMapper.class);
			job5.setMapOutputKeyClass(Text.class);
			job5.setMapOutputValueClass(LongWritable.class);

			job5.setReducerClass(AirportTaxiTimeAvgReducer.class);
			job5.setOutputKeyClass(Text.class);
			job5.setOutputValueClass(DoubleWritable.class);

			FileInputFormat.addInputPath(job5, new Path(arg0[0]));
			if (fs.exists(new Path(TEMP_OUTPUT_PATH2))) {
				fs.delete(new Path(TEMP_OUTPUT_PATH2), true);
			}
			FileOutputFormat.setOutputPath(job5, new Path(TEMP_OUTPUT_PATH2));

			job5.waitForCompletion(true);

			/*
			 *
			 * Airport Taxi Out time second job - Max
			 */
			Job job6 = Job.getInstance(getConf());
			job6.setJarByClass(getClass());

			job6.setMapperClass(FindingTopKMapper.class);
			job6.setMapOutputKeyClass(DoubleWritable.class);
			job6.setMapOutputValueClass(Text.class);

			job6.setNumReduceTasks(1);

			job6.setCombinerClass(FindingTop3Combiner.class);
			job6.setReducerClass(FindingTop3Reducer.class);
			job6.setSortComparatorClass(LongWritable.DecreasingComparator.class);

			job6.setOutputKeyClass(Text.class);
			job6.setOutputValueClass(DoubleWritable.class);

			FileInputFormat.addInputPath(job6, new Path(TEMP_OUTPUT_PATH2));
			if (fs.exists(new Path(arg0[1] + "_AirportTaxiOutTime_Max"))) {
				fs.delete(new Path(arg0[1] + "_AirportTaxiOutTime_Max"), true);
			}
			FileOutputFormat.setOutputPath(job6, new Path(arg0[1] + "_AirportTaxiOutTime_Max"));

			job6.waitForCompletion(true);
			/*
			 *
			 * Airport Taxi Out time third job - Min
			 */
			Job job7 = Job.getInstance(getConf());
			job7.setJarByClass(getClass());

			job7.setMapperClass(FindingTopKMapper.class);
			job7.setMapOutputKeyClass(DoubleWritable.class);
			job7.setMapOutputValueClass(Text.class);

			job7.setNumReduceTasks(1);

			job7.setCombinerClass(FindingTop3Combiner.class);
			job7.setReducerClass(FindingTop3Reducer.class);
			job7.setSortComparatorClass(DoubleWritable.Comparator.class);

			job7.setOutputKeyClass(Text.class);
			job7.setOutputValueClass(DoubleWritable.class);

			FileInputFormat.addInputPath(job7, new Path(TEMP_OUTPUT_PATH2));
			if (fs.exists(new Path(arg0[1] + "_AirportTaxiOutTime_Min"))) {
				fs.delete(new Path(arg0[1] + "_AirportTaxiOutTime_Min"), true);
			}
			FileOutputFormat.setOutputPath(job7, new Path(arg0[1] + "_AirportTaxiOutTime_Min"));

			job7.waitForCompletion(true);

			// we will use ArrDelay for finding the 3 airlines with the highest
			// and lowest probability, respectively, for being on schedule.
			// if ArrDelay value is greater than 10 then it is considered that
			// it is not on time

			/*
			 * 
			 * Airlines reliability
			 */
			Job job8 = Job.getInstance(getConf());
			job8.setJarByClass(getClass());

			job8.setMapperClass(FlightArrDelayMapper1.class);
			job8.setMapOutputKeyClass(Text.class);
			job8.setMapOutputValueClass(LongWritable.class);

			job8.setSortComparatorClass(StringComparator.class);

			job8.setReducerClass(FlightArrDelayReducer1.class);
			job8.setOutputKeyClass(Text.class);
			job8.setOutputValueClass(LongWritable.class);

			FileInputFormat.setInputPaths(job8, new Path(arg0[0]));
			if (fs.exists(new Path(arg0[1] + "_AirlinesBeingOnSchedule"))) {
				fs.delete(new Path(arg0[1] + "_AirlinesBeingOnSchedule"), true);
			}

			FileOutputFormat.setOutputPath(job8, new Path(arg0[1] + "_AirlinesBeingOnSchedule"));
			// TODO Auto-generated method stub

			return job8.waitForCompletion(true) ? 0 : 1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new FlightDataAnalysisMain(), args));
	}

	public static class DecreasingComparator extends Comparator {

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			DoubleWritable a1 = (DoubleWritable) a;
			DoubleWritable b1 = (DoubleWritable) b;
			return (a1.get() < b1.get() ? -1 : (a1.get() == b1.get() ? 0 : 1));
		}

	}

	public static class StringComparator extends WritableComparator {

		public StringComparator() {
			super(Text.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

			try {

				String v1 = Text.decode(b1, s1, l1);
				String v2 = Text.decode(b2, s2, l2);

				return v1.compareTo(v2);

			} catch (Exception e) {
				throw new IllegalArgumentException(e);
			}
		}
	}
}
