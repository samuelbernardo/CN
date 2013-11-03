package project.mapred;

import java.io.IOException;
import java.util.*; 	

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import project.mapred.types.intermediate.*;

/**
 * Class that holds the implementation of the Map, Reduce and auxiliary classes.
 */
public class Runner {

	/**
	 * Class defining the map method.
	 */
	public static class Map 
	extends MapReduceBase 
	implements Mapper<LongWritable, Text, IntermediateKey, IntermediateValue> {

		/**
		 * Possible events.
		 */
		public static final int PHONE_JOINS_NETWORK = 4; 
		public static final int PHONE_LEAVES_NETWORK = 5; 
		public static final int PHONE_JOINS_CELL = 2; 
		public static final int PHONE_LEAVES_CELL = 3;
		public static final int PHONE_INIT_CALL = 6;
		public static final int PHONE_TERM_CALL = 7;
		public static final int PHONE_PINGS_CELL = 8;

		/**
		 * Constants.
		 */
		public static final String YES = "Y";
		public static final String NO = "N";
		public static final char ENTER = '+';
		public static final char LEAVE = '-';
		public static final String ZERO = "0";
		public static final int SECONDS_IN_HOUR = 60*60;
		public static final int SECONDS_IN_DAY = SECONDS_IN_HOUR*60;
		public static final int HOURS_IN_DAY = 24;


		/**
		 * Map implementation.
		 * @param key - by default, the input file cursor. 
		 * @param value - by default, a complete line from the input file.
		 * @param output - the collector.
		 * @param reporter - could be used to report progress (not in use).
		 * @throws IOException
		 */
		public void map(
				LongWritable key, 
				Text value, 
				OutputCollector<IntermediateKey, IntermediateValue> output, 
				Reporter reporter) throws IOException {

			String[] line = value.toString().trim().split(", ");
			String 	cell = line[0], 
					date = line[1], 
					time = line[2],
					event = line[3], 
					phone = line[4];
			List<Text> list = new ArrayList<Text>();
			IntermediateValue iv = null;
			int nSecs = -1;

			switch (Integer.parseInt(event)) {
			case PHONE_JOINS_NETWORK:
				nSecs = this.getNumberSeconds(time);
				list.add(new Text(new Integer(nSecs).toString()));
				list.add(new Text(Map.ZERO));
				list.add(new Text(Map.YES));
				list.add(new Text(Map.ZERO));
				output.collect(new IntermediateKey(date, time, phone), new OffIntermediateValue(list));
				break;
			case PHONE_LEAVES_NETWORK:
				nSecs = this.getNumberSeconds(time);
				list.add(new Text(new Integer(nSecs).toString()));
				list.add(new Text(Map.ZERO));
				list.add(new Text(Map.NO));
				list.add(new Text(new Integer(Map.SECONDS_IN_DAY - nSecs).toString()));
				output.collect(new IntermediateKey(date, time, phone), new OffIntermediateValue(list));
				break;
			case PHONE_JOINS_CELL:
				list.add(new Text(Map.ENTER + phone)); 
				iv = new PresentIntermediateValue(list);
				output.collect(new IntermediateKey(date, time, cell+":"+time.substring(0, 2)), iv);
				list = new ArrayList<Text>(); 
				list.add(new Text(cell));
				output.collect(new IntermediateKey(date, time, phone), new CellsIntermediateValue(list));
				break;
			case PHONE_LEAVES_CELL:
				list.add(new Text(Map.LEAVE + phone));
				output.collect(new IntermediateKey(date, time, cell+":"+time.substring(0, 2)), new PresentIntermediateValue(list));
				break;
			case PHONE_INIT_CALL:
			case PHONE_TERM_CALL:
			case PHONE_PINGS_CELL:
				list.add(new Text(Map.ENTER + phone));
				output.collect(new IntermediateKey(date, time, cell+":"+time.substring(0, 2)), new PresentIntermediateValue(list));
				// If the first hour is gone, we don't need this "still alive"
				// messages. 
				if (this.getNumberSeconds(time) >= SECONDS_IN_HOUR) {
					list = new ArrayList<Text>(); 
					list.add(new Text(cell));
					output.collect(new IntermediateKey(date, time, phone), new CellsIntermediateValue(list));
				}
				break;
			}
		}

		/**
		 * Auxiliary method that will convert a string representing time in the
		 * number of seconds since 0h0m0s.
		 * @param time - string like 17:54.01
		 * @return - number of seconds.
		 */
		public int getNumberSeconds(String time) {
			Integer hours = Integer.parseInt(time.substring(0,2));
			Integer mins = Integer.parseInt(time.substring(2,4));
			Integer secs = Integer.parseInt(time.substring(4,6));
			secs += (hours*60 + mins)*60;
			return secs;
		}
	}

	/**
	 * Class defining the reduce method.
	 */
	public static class Reduce 
	extends MapReduceBase 
	implements Reducer<IntermediateKey, IntermediateValue, IntermediateKey, IntermediateValue> {

		/**
		 * Reduce implementation.
		 * @param key - the key for the given values.
		 * @param it - values' iterator.
		 * @param output - the collector.
		 * @param reporter - could be used to report progress (not in use).
		 * @throws IOException
		 */
		public void reduce(
				IntermediateKey key, 
				Iterator<IntermediateValue> it, 
				OutputCollector<IntermediateKey, IntermediateValue> output, 
				Reporter reporter) throws IOException {
			// FIXME: offintermediate and cells will colide
			IntermediateValue value = null;
			for(value = it.next(); it.hasNext(); value.merge(it.next()));
			output.collect(key, value);
		}
	}

	/**
	 * Class defining how hadoop should partition keys.
	 */
	public static final class Partition
	implements Partitioner<IntermediateKey, IntermediateValue> {

		/**
		 * Not necessary.
		 */
		@Override
		public void configure(JobConf arg0) {}

		/**
		 * Method that performs the partition. 
		 * Note: this method indicates that partitions should be made by
		 * date and id. The hashCode procedure is the default behavior done by
		 * hadoop.
		 * @param k - the key for the given value.
		 * @param v - the value for the given key.
		 * @param numReduceTasks - the name tells everything. 
		 */
		@Override
		public int getPartition(
				IntermediateKey k, IntermediateValue v, int numReduceTasks) {
			return k.getDateId().hashCode() % numReduceTasks;
		}

	}
	
	/**
	 * Class defining how hadoop should group values (before calling reduce).
	 */
	public static final class GroupingComparator extends Text.Comparator {

		/**
		 * Nice and efficient way to reuse functionality.
		 */
		@Override
		public int compare(
				byte[] b1, int start1, int length1, 
				byte[] b2, int start2, int length2) {
			return super.compare(
					b1, start1, length1 - IntermediateKey.TIME_SIZE, 
					b2, start2, length2 - IntermediateKey.TIME_SIZE);
		}
	}
	

	/**
	 * Main
	 * @param args  
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Runner.class);
		conf.setJobName("mobile-net");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setOutputKeyComparatorClass(Text.Comparator.class);
		conf.setOutputValueGroupingComparator(GroupingComparator.class);
		conf.setPartitionerClass(Partition.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
