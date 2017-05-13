

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirportTaxiInTimeMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	public void map(LongWritable lineOffSet, Text record, Context context) throws IOException, InterruptedException {
		if (record != null) {
			String[] strArr = record.toString().split(",");
			if (strArr != null && strArr.length > 19 && strArr[0] != null && strArr[16] != null && strArr[19] != null) {
				// do not process first record as they are column headers
				if (strArr[0] != null && !strArr[0].equals("Year")) {
					String origin = strArr[16];
					// String dest = strArr[17];
					String texiInTimeStr = strArr[19];
					// String texiOutTimeStr = strArr[20];
					if (origin != null && !origin.trim().equals("") && texiInTimeStr != null
							&& !texiInTimeStr.trim().equals("")) {
						// if (dest != null && !dest.trim().equals("") &&
						// texiOutTimeStr
						// != null && !texiOutTimeStr.trim().equals("")) {
						try {
							long texiInTime = Long.parseLong(texiInTimeStr);
							context.write(new Text(origin), new LongWritable(texiInTime));
						} catch (Exception e) {
							// just ignore exception and process to next record
							//e.printStackTrace();
						}
						// context.write(new Text(dest), new
						// LongWritable(Long.parseLong(texiOutTimeStr)));
					}
				}
			}
		}
	}
}
