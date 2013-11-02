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

public class Runner {

	/**
	 * TODO
	 */
	public static class Map 
	extends MapReduceBase 
	implements Mapper<LongWritable, Text, IntermediateKey, IntermediateValue> {

		/**
		 * Events to process
		 */
		public static final int PHONE_JOINS_NETWORK = 4; 
		public static final int PHONE_LEAVES_NETWORK = 5; 
		public static final int PHONE_JOINS_CELL = 2; 
		public static final int PHONE_LEAVES_CELL = 3;
		public static final int PHONE_INIT_CALL = 6;
		public static final int PHONE_TERM_CALL = 7;
		public static final int PHONE_PINGS_CELL = 8;

		/**
		 * Constants used for values.
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
		 * TODO
		 * @param key
		 * @param value
		 * @param output
		 * @param reporter
		 * @throws IOException
		 */
		public void map(
				LongWritable key, 
				Text value, 
				OutputCollector<IntermediateKey, IntermediateValue> output, 
				Reporter reporter) throws IOException {

			String[] line = value.toString().split(", ");
			List<Text> list = new ArrayList<Text>();
			IntermediateValue iv;

			switch (Integer.parseInt(line[3])) {
			case PHONE_JOINS_NETWORK:
				list.add(new Text(new Integer(
						this.getNumberSeconds(line[2])).toString()));
				list.add(new Text(Map.ZERO));
				list.add(new Text(Map.YES));
				list.add(new Text(Map.ZERO));
				iv = new OffIntermediateValue(list);
				this.collect(output, line[1], line[2], line[4], iv);
				break;
			case PHONE_LEAVES_NETWORK: 
				list.add(new Text(new Integer(
						this.getNumberSeconds(line[2])).toString()));
				list.add(new Text(Map.ZERO));
				list.add(new Text(Map.NO));
				list.add(new Text(new Integer(
						Map.SECONDS_IN_DAY - this.getNumberSeconds(line[2])).toString()));
				iv = new OffIntermediateValue(list);
				this.collect(output, line[1], line[2], line[4], iv); 	    	   
				break;
			case PHONE_JOINS_CELL:
				list.add(new Text(Map.ENTER + line[4])); 
				iv = new PresentIntermediateValue(list);
				this.collect(output, line[1], line[2], line[0]+line[2].substring(0, 2), iv);
				list = new ArrayList<Text>(); 
				list.add(new Text(line[0]));
				iv = new CellsIntermediateValue(list);
				this.collect(output, line[1], line[2], line[4], iv);
				break;
			case PHONE_LEAVES_CELL:
				list.add(new Text(Map.LEAVE + line[4]));
				iv = new PresentIntermediateValue(list);
				this.collect(output, line[1], line[2], line[0]+line[2].substring(0, 2), iv);
				break;
			case PHONE_INIT_CALL:
			case PHONE_TERM_CALL:
			case PHONE_PINGS_CELL:
				list.add(new Text(Map.ENTER + line[4]));
				iv = new PresentIntermediateValue(list);
				this.collect(output, line[1], line[2], line[0]+line[2].substring(0, 2), iv);
				// If the first hour is gone, we don't need this "still alive"
				// messages. 
				if (this.getNumberSeconds(line[2]) >= SECONDS_IN_HOUR) {
					list = new ArrayList<Text>(); 
					list.add(new Text(line[0]));
					iv = new CellsIntermediateValue(list);
					this.collect(output, line[1], line[2], line[4], iv);
				}
				break;
			}
		}

		/**
		 * TODO
		 * @param time
		 * @return
		 * @throws IOException 
		 */
		public void collect(
				OutputCollector<IntermediateKey, IntermediateValue> output, 
				String date, 
				String time, 
				String id, 
				IntermediateValue value) throws IOException {
			output.collect(new IntermediateKey(date, time, id), value);
		}

		/**
		 * TODO
		 * @param time
		 * @return
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
	 * TODO
	 */
	public static class Reduce 
	extends MapReduceBase 
	implements Reducer<IntermediateKey, IntermediateValue, IntermediateKey, IntermediateValue> {

		/**
		 * TODO
		 */
		public void reduce(
				IntermediateKey key, 
				Iterator<IntermediateValue> it, 
				OutputCollector<IntermediateKey, IntermediateValue> output, 
				Reporter reporter) throws IOException {
			// FIXME: offintermediate and cells will colide
			IntermediateValue value = it.next();
			while(it.hasNext()) { value.merge(it.next()); }
		}
	}

	/**
	 * TODO
	 */
	public static final class Partition 
	implements Partitioner<IntermediateKey, IntermediateValue> {

		@Override
		public void configure(JobConf arg0) {}

		@Override
		public int getPartition(
				IntermediateKey k, IntermediateValue v, int numReduceTasks) {
			return k.getDateId().hashCode() % numReduceTasks;
		}

	}
	
	/**
	 * TODO
	 *
	 */
	public static final class GroupingComparator implements RawComparator<IntermediateKey> {

		@Override
		public int compare(
				byte[] k1, int start1, int length1, byte[] k2,int start2, int length2) {
			byte[] b1 = {k1[start1+IntermediateKey.DATE_BEG], k1[start1+IntermediateKey.TIME_END]};
			return new Text(b1).compareTo(k2, start2, IntermediateKey.TIME_END);
		}

		@Override
		public int compare(IntermediateKey k1, IntermediateKey k2) {
			return this.compare(k1.getBytes(), 0, k1.getLength(), k2.getBytes(), 0, k2.getLength());
		}
	}
	
	/**
	 * 
	 *
	 */
	public static final class KeyComparator implements RawComparator<IntermediateKey>{
		
		@Override
		public int compare(
				byte[] k1, int start1, int length1, byte[] k2,int start2, int length2) {
			byte[] b1 = {k1[start1], k1[length1-1]};
			return new Text(b1).compareTo(k2, start2, length2);
		}
		
		@Override
		public int compare(IntermediateKey k1, IntermediateKey k2) {
			return k1.compareTo(k2);
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Runner.class);
		conf.setJobName("wordcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

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
