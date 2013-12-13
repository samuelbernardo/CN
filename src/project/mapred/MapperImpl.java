package project.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import project.mapred.types.intermediate.IntermediateKey;
import project.mapred.types.intermediate.IntermediateValue;

public class MapperImpl {
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
		public static final String DUMMY_TIME = "00:00:00";
		public static final String ENTER = "+";
		public static final String LEAVE = "-";
		public static final String ZERO = "0";
		public static final int SECONDS_IN_HOUR = 60*60;
		public static final int SECONDS_IN_DAY = SECONDS_IN_HOUR*24;
		public static final int HOURS_IN_DAY = 24;
		public static final String VISITED_CELLS = new String(new Integer(Runner.VISITED_CELLS).toString());
		public static final String PRESENT_PHONES = new String(new Integer(Runner.PRESENT_PHONES).toString());
		public static final String OFFLINE_TIME = new String(new Integer(Runner.OFFLINE_TIME).toString());


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

			//try {
			String[] line = value.toString().trim().split(",");
			String  cell = line[0],
					date = line[1],
					time = line[2],
					event = line[3],
					phone = event.equals("0") ? null : line[4];
			
			List<Text> list = new ArrayList<Text>();
			int nSecs = -1;

			switch (Integer.parseInt(event)) {
			case PHONE_JOINS_NETWORK:
				nSecs = this.getNumberSeconds(time);
				list.add(new Text(Map.ENTER));
				list.add(new Text(new Integer(nSecs).toString()));
				output.collect(
						new IntermediateKey(OFFLINE_TIME, date, time, phone),
						new IntermediateValue(list));
				break;
			case PHONE_LEAVES_NETWORK:
				nSecs = this.getNumberSeconds(time);
				list.add(new Text(Map.LEAVE));
				list.add(new Text(new Integer(nSecs).toString()));
				output.collect(
						new IntermediateKey(OFFLINE_TIME, date, time, phone), 
						new IntermediateValue(list));
				break;
			case PHONE_JOINS_CELL:
				/*System.out.print("Mapper line:");
				for(String txt : line) System.out.print(" "+txt);
				System.out.print("\n");*/
				// PRESENT_PHONES
				list.add(new Text(time));
				list.add(new Text(Map.ENTER + phone)); 
				output.collect(
						new IntermediateKey(
								PRESENT_PHONES, 
								date, 
								DUMMY_TIME, 
								cell+":"+time.substring(0, 2)), 
						new IntermediateValue(list));
				// VISITED_CELLS
				list = new ArrayList<Text>(); 
				list.add(new Text(time+";"+cell));
				output.collect(new IntermediateKey(VISITED_CELLS, date, time, phone), new IntermediateValue(list));
				break;
			case PHONE_LEAVES_CELL:
				/*System.out.print("Mapper line:");
				for(String txt : line) System.out.print(" "+txt);
				System.out.print("\n");*/
				// PRESENT_PHONES
				list.add(new Text(time));
				list.add(new Text(Map.LEAVE + phone));
				output.collect(
						new IntermediateKey(
								PRESENT_PHONES, 
								date, 
								DUMMY_TIME, 
								cell+":"+time.substring(0, 2)), 
						new IntermediateValue(list));
				// VISITED_CELLS
				// Mark that the phone was in this cell before leaving.
				if (this.getNumberSeconds(time) <= SECONDS_IN_HOUR) {
					list = new ArrayList<Text>();
					list.add(new Text(time+";"+cell));
					output.collect(new IntermediateKey(VISITED_CELLS, date, time, phone), new IntermediateValue(list));
				}
				break;
			case PHONE_INIT_CALL:
			case PHONE_TERM_CALL:
			case PHONE_PINGS_CELL:
				/*System.out.print("Mapper line:");
				for(String txt : line) System.out.print(" "+txt);
				System.out.print("\n");*/
				// PRESENT_PHONES
				list.add(new Text(time));
				list.add(new Text(Map.LEAVE + phone));
				output.collect(
						new IntermediateKey(
								PRESENT_PHONES, 
								date, 
								DUMMY_TIME, 
								cell+":"+time.substring(0, 2)), 
						new IntermediateValue(list));
				// VISITED_CELLS
				// Mark that the phone is in this cell. 
				if (this.getNumberSeconds(time) <= SECONDS_IN_HOUR) {
					list = new ArrayList<Text>();
					list.add(new Text(time+";"+cell));
					output.collect(new IntermediateKey(VISITED_CELLS, date, time, phone), new IntermediateValue(list));
				}
				break;
			}
			/*}
			catch(Exception e) {
				System.out.println("Mapper value: "+value);
				System.out.println("Exception catched: "+e);
			}*/
			
		}

		/**
		 * Auxiliary method that will convert a string representing time in the
		 * number of seconds since 0h0m0s.
		 * @param time - string like 17:54.01
		 * @return - number of seconds.
		 */
		public int getNumberSeconds(String time) {
			Integer hours = Integer.parseInt(time.substring(0,2));
			Integer mins = Integer.parseInt(time.substring(3,5));
			Integer secs = Integer.parseInt(time.substring(6,8));
			secs += (hours*60 + mins)*60;
			return secs;
		}
	}
}
