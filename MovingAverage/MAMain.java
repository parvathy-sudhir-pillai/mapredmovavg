package MovingAverage;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MAMain extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		System.out.println("\n\n----Demand Forecasting with Moving Average----\n");

		Job majob = new Job(getConf(),"DemandForecastMovingAverage");
		majob.setJarByClass(MAMain.class);
	
		majob.setMapOutputKeyClass(TimeSeriesKey.class);
		majob.setMapOutputValueClass(TimeSeriesDataPoint.class);

		majob.setMapperClass(MAMapper.class);
		majob.setReducerClass(MAReducer.class);

		majob.setPartitionerClass(NaturalKeyPartitioner.class);
		majob.setSortComparatorClass(CompositeKeyComparator.class);
		majob.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);

		
		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-ws".equals(args[i])) {

					majob.getConfiguration().set("MovingAverage.windowsize", args[++i]);

				} 
				else if ("-wss".equals(args[i])) {

					majob.getConfiguration().set("MovingAverage.windowstepsize", args[++i]);

				}
				else {

					other_args.add(args[i]);

				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of "+ args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from "+ args[i - 1]);
				return printUsage();
			}
		}
		
		if (other_args.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: "
					+ other_args.size() + " instead of 2.");
			return printUsage();
		}
		
		majob.setInputFormatClass(TextInputFormat.class);
		majob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(majob, other_args.get(0));
		FileOutputFormat.setOutputPath(majob, new Path(other_args.get(1)));

		majob.waitForCompletion(true);

		return 0;
	}

	static int printUsage() {
		System.out.println("MovingAverageJob [-m <maps>] [-r <reduces>] <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new MAMain(),args);
		System.exit(res);

	}

}