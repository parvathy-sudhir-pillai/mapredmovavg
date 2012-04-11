package MovingAverage;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.RawComparator;

public class CompositeKeyComparator implements RawComparator<TimeSeriesKey>{

	public int compare(TimeSeriesKey k1, TimeSeriesKey k2) {

		int cmp = k1.getRegion().compareTo(k2.getRegion());
		if (cmp != 0) {
			return cmp;
		}

		return k1.getTimestamp() == k2.getTimestamp() ? 0 : (k1.getTimestamp() < k2.getTimestamp() ? -1 : 1);

	}

	@Override
	public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4,int arg5) {
		return 0;
	}

	
}