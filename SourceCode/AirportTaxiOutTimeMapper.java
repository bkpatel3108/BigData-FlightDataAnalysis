

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirportTaxiOutTimeMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	public void map(LongWritable lineOffSet, Text record, Context context) throws IOException, InterruptedException {
		String[] strArr = record.toString().split(",");
		// do not process first record as they are column headers
		if (strArr[0] != null && !strArr[0].equals("Year")) {
			// String origin = strArr[16];
			String dest = strArr[17];
			// String texiInTimeStr = strArr[19];
			String texiOutTimeStr = strArr[20];
			// if (origin != null && !origin.trim().equals("") &&
			// texiInTimeStr != null
			// && !texiInTimeStr.trim().equals("")) {
			if (dest != null && !dest.trim().equals("") && texiOutTimeStr != null
					&& !texiOutTimeStr.trim().equals("")) {
				try {
					long texiOutTime = Long.parseLong(texiOutTimeStr);
					context.write(new Text(dest), new LongWritable(texiOutTime));
				} catch (Exception e) {
					// just ignore exception and process to next record
					// e.printStackTrace();
				}
			}
		}

	}
}
