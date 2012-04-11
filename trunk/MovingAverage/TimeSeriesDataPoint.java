package MovingAverage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.Writable;

public class TimeSeriesDataPoint implements Writable,Comparable<TimeSeriesDataPoint> {
	
	public long lTimestamp;
	public float fOfftake;

	private static final String DATE_FORMAT = "yyyyMMdd";
	private static SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);

	public void readFields(DataInput in) throws IOException {

		this.lTimestamp = in.readLong();
		this.fOfftake = in.readFloat();
	}

	public static TimeSeriesDataPoint read(DataInput in) throws IOException {

		TimeSeriesDataPoint p = new TimeSeriesDataPoint();
		p.readFields(in);
		return p;

	}

	public String getDate() {

		return String.valueOf(lTimestamp);

	}

	public void copy(TimeSeriesDataPoint source) {

		this.lTimestamp = source.lTimestamp;
		this.fOfftake = source.fOfftake;

	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeLong(this.lTimestamp);
		out.writeFloat(this.fOfftake);

	}

	@Override
	public int compareTo(TimeSeriesDataPoint oOther) {
		if (this.lTimestamp < oOther.lTimestamp) {
			return -1;
		} 
		else if (this.lTimestamp > oOther.lTimestamp) {
			return 1;
		}
		return 0;
	}

}