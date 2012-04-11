package MovingAverage;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class MAMapper extends Mapper<LongWritable, Text, TimeSeriesKey, TimeSeriesDataPoint> {

	static enum Parse_Counters {BAD_PARSE};

	private final TimeSeriesKey key = new TimeSeriesKey();
	private final TimeSeriesDataPoint val = new TimeSeriesDataPoint();

	private static final Logger logger = Logger.getLogger(MAMapper.class);

	public void map(LongWritable inkey, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();

		SmartMeterDataPoint rec = SmartMeterDataPoint.parse(line);

		if (rec != null) {

			key.set(rec.getRegion(), rec.getDate());

			val.fOfftake = rec.getOfftake();
			val.lTimestamp = rec.getDate();
			context.write(key, val);
		} 
		
		else {

			context.getCounter(Parse_Counters.BAD_PARSE);
			logger.error("BAD PARSE");
		}

	}

}