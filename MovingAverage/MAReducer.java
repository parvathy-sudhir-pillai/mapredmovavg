package MovingAverage;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MAReducer extends Reducer<TimeSeriesKey, TimeSeriesDataPoint, Text, Text> {

	static enum PointCounters {POINTS_SEEN, POINTS_ADDED_TO_WINDOWS, MOVING_AVERAGES_CALCD};

	static long day_in_ms = 24 * 60 * 60 * 1000;

	public void reduce(TimeSeriesKey key, Iterable<TimeSeriesDataPoint> values, Context context)throws IOException, InterruptedException {

		float point_sum = 0;
		float moving_avg = 0;

		int iWindowSize = context.getConfiguration().getInt("MovingAverage.windowsize", 30);
		int iWindowStepSize = context.getConfiguration().getInt("MovingAverage.windowstepsize", 1);

		Text out_key = new Text();
		Text out_val = new Text();

		SlidingWindow sliding_window = new SlidingWindow(iWindowSize,iWindowStepSize);

		for(TimeSeriesDataPoint next_point:values) {

			if (sliding_window.isWindowFull() == false) {

				context.getCounter(PointCounters.POINTS_ADDED_TO_WINDOWS);

				TimeSeriesDataPoint p_copy = new TimeSeriesDataPoint();
				p_copy.copy(next_point);

				try {
					sliding_window.addPoint(p_copy);
				} catch (Exception e) {
					e.printStackTrace();
				}

			}

			if (sliding_window.isWindowFull()) {

				context.getCounter(PointCounters.MOVING_AVERAGES_CALCD);
				LinkedList<TimeSeriesDataPoint> oWindow = sliding_window.getCurrentWindow();
				String strBackDate = oWindow.getLast().getDate();

				// 1. ---------- compute the moving average -----------

				out_key.set("Region: " + key.getRegion() + ", Date: " + strBackDate);

				point_sum = 0;

				for (int x = 0; x < oWindow.size(); x++) {

					point_sum += oWindow.get(x).fOfftake;

				} 

				moving_avg = point_sum / oWindow.size();

				out_val.set("Moving Average: " + moving_avg);

				context.write(out_key, out_val);

				// 2. -------------- step window forward ----------------

				sliding_window.SlideWindowForward();

			}

		} 

		out_key.set("debug > " + key.getRegion() + " --------- end of region -------------");
		out_val.set("");

		context.write(out_key, out_val);

	} 

}