package MovingAverage;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

@SuppressWarnings("deprecation")
public class NaturalKeyPartitioner extends Partitioner<TimeSeriesKey, TimeSeriesDataPoint> {

	@Override
	public int getPartition(TimeSeriesKey key, TimeSeriesDataPoint value, int numPartitions) {
		return Math.abs(key.getRegion().hashCode() * 127) % numPartitions;
	}
}