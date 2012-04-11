package MovingAverage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TimeSeriesKey implements WritableComparable<TimeSeriesKey>{

	private String sRegion = "";
	private long lTimestamp = 0;

	public void set(String strGroup, long lTS) {

		this.sRegion = strGroup;
		this.lTimestamp = lTS;

	}

	public String getRegion() {
		return this.sRegion;
	}

	public long getTimestamp() {
		return this.lTimestamp;
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		this.sRegion = in.readUTF();
		this.lTimestamp = in.readLong();

	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeUTF(sRegion);
		out.writeLong(this.lTimestamp);
	}

	@Override
	public int compareTo(TimeSeriesKey other) {

		if (this.sRegion.compareTo(other.sRegion) != 0) {
			return this.sRegion.compareTo(other.sRegion);
		} 
		else if (this.lTimestamp != other.lTimestamp) {
			return lTimestamp < other.lTimestamp ? -1 : 1;
		}
		else {
			return 0;
		}

	}
}