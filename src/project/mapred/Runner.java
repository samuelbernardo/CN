package project.mapred;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*; 	

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

import project.mapred.types.intermediate.*;

/**
 * Class that holds the implementation of the Map, Reduce and auxiliary classes.
 */
public class Runner {


	/**
	 * Constants. 
	 * Note: this constants are integer because we need their string
	 * representation also.
	 */
	public static final int VISITED_CELLS = 0;
	public static final int PRESENT_PHONES = 1;
	public static final int OFFLINE_TIME = 2;

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

			String[] line = value.toString().trim().split(",");
			String 	cell = line[0], 
					date = line[1], 
					time = line[2],
					event = line[3], 
					phone = event.equals("0") ? null : line[4];
			List<Text> list = new ArrayList<Text>();
			int nSecs = -1;

			switch (Integer.parseInt(event)) {
			case PHONE_JOINS_NETWORK:
				nSecs = this.getNumberSeconds(time);
				list.add(new Text(new Integer(nSecs).toString()));
				list.add(new Text(new Integer(nSecs).toString()));
				list.add(new Text(Map.YES));
				list.add(new Text(Map.ZERO));
				output.collect(new IntermediateKey(OFFLINE_TIME, date, time, phone),new IntermediateValue(list));
				break;
			case PHONE_LEAVES_NETWORK:
				nSecs = this.getNumberSeconds(time);
				list.add(new Text(new Integer(nSecs).toString()));
				list.add(new Text(Map.ZERO));
				list.add(new Text(Map.NO));
				list.add(new Text(new Integer(Map.SECONDS_IN_DAY - nSecs).toString()));
				output.collect(new IntermediateKey(OFFLINE_TIME, date, time, phone), new IntermediateValue(list));
				break;
			case PHONE_JOINS_CELL:
				list.add(new Text(Map.ENTER + phone)); 
				output.collect(
						new IntermediateKey(
								PRESENT_PHONES, 
								fixPresentPhonesDate(date, time), 
								time, 
								cell+":"+fixPresentPhonesHour(time.substring(0, 2))), 
						new IntermediateValue(list));
				list = new ArrayList<Text>(); 
				list.add(new Text(cell));
				output.collect(new IntermediateKey(VISITED_CELLS, date, time, phone), new IntermediateValue(list));
				break;
			case PHONE_LEAVES_CELL:
				list.add(new Text(Map.LEAVE + phone));
				output.collect(
						new IntermediateKey(
								PRESENT_PHONES, 
								fixPresentPhonesDate(date, time), 
								time, 
								cell+":"+fixPresentPhonesHour(time.substring(0, 2))), 
						new IntermediateValue(list));
				break;
			case PHONE_INIT_CALL:
			case PHONE_TERM_CALL:
			case PHONE_PINGS_CELL:
				list.add(new Text(Map.ENTER + phone));
				output.collect(
						new IntermediateKey(
								PRESENT_PHONES, 
								fixPresentPhonesDate(date, time), 
								time, 
								cell+":"+fixPresentPhonesHour(time.substring(0, 2))), 
						new IntermediateValue(list));
				// If the first hour is gone, we don't need this "still alive"
				// messages. 
				if (this.getNumberSeconds(time) <= SECONDS_IN_HOUR) {
					list = new ArrayList<Text>();
					// This cell is marked to detect possible repetitions.
					// Ugly solution though.
					list.add(new Text(cell+"?"));
					output.collect(new IntermediateKey(VISITED_CELLS, date, time, phone), new IntermediateValue(list));
				}
				break;
			}
		}
		
		/**
		 * Increments the date by one day if the hour equals to 23.
		 * @param date
		 * @param hour
		 * @return - the new date
		 * @throws IOException 
		 * @throws ParseException
		 */
		String fixPresentPhonesDate(String date, String time) throws IOException {
			if (time.substring(0, 2).equals("23")) {
				try {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				Calendar cal = Calendar.getInstance();
				cal.setTime(sdf.parse(date));
				cal.add(Calendar.DATE, 1);
				date = sdf.format(cal.getTime());
				} catch (ParseException e) {
					throw new IOException(e);
				}
			}
			return date;
		}
		
		/**
		 * Increments the hour.
		 * @param hour
		 * @return
		 */
		String fixPresentPhonesHour(String hour) 
		{ return String.format("%02d", (Integer.parseInt(hour)+1)%HOURS_IN_DAY); }

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
				IntermediateKey k, 
				Iterator<IntermediateValue> it, 
				OutputCollector<IntermediateKey, IntermediateValue> output, 
				Reporter reporter) throws IOException {
			IntermediateValue v = new IntermediateValue(it.next().getValues());
			switch (Integer.parseInt(k.getQuery())) {
			case VISITED_CELLS:
				for(; it.hasNext(); reduceVisitedCells(v, it.next()));
				break;
			case PRESENT_PHONES:
				for(; it.hasNext(); reducePresentPhones(v, it.next()));
				break;
			case OFFLINE_TIME:
				for(; it.hasNext(); reduceOfflineTime(v, it.next()));
				break;
			}
			
			output.collect(k, v);
		}

		/**
		 * Auxiliary method that will reduce the VisitedCelss pairs.
		 * The list will have the following format: <cell1,...,cellN>
		 * @param iv1
		 * @param iv2
		 */
		private void reduceVisitedCells(IntermediateValue iv1, IntermediateValue iv2) {
			// FIXME
			// WARNING: there could be an issue here. If in the first hour of a day,
			// the phone leaves the cell before pinging it, we will not record that
			// the phone was in that cell.
			// UGLY SOLUTION: see if the disconnection happens within the first 
			// hour and reproduce a ping.
			for(Iterator<Text> i = iv2.getValues().iterator(); i.hasNext();) {
				String tmp = i.next().toString();
				// means that it was not triggered by a join event. 
				if(tmp.endsWith("?"))
				{
					// get the string without the '?'
					String tmp2 = tmp.substring(0, tmp.length()-1);
					if (!iv1.getValues().contains(new Text(tmp2))) 
					{ iv1.getValues().add(new Text(tmp2)); }
				}
				else { iv1.getValues().add(new Text(tmp)); }
			}
		}

		/**
		 * Auxiliary method that will reduce the PresentPhones pairs.
		 * The list will have the following format: <[+/-]phone1,...,[+/-]phoneN>
		 * @param iv1
		 * @param iv2
		 */
		private void reducePresentPhones(IntermediateValue iv1, IntermediateValue iv2) {
			for (Iterator<Text> i = iv2.getValues().iterator(); i.hasNext();) {
				Text tmp = i.next();
				// if contains, nothing to do.
				if (iv1.getValues().contains(tmp)) { continue; }
				else {
					// change + to - or - to + 
					// FIXME: inefficient code kills trees!
					Text itmp = new Text(tmp);
					if(tmp.charAt(0) == '+') { itmp.set(tmp.toString().replace('+', '-')); }
					else { itmp.set(tmp.toString().replace('-', '+')); }
					// if contains the inverse
					if (iv1.getValues().contains(itmp)) {
						iv1.getValues().remove(tmp);
						iv1.getValues().add(itmp);
					}
					else { iv1.getValues().add(tmp); }
				}			
			}
		}

		/**
		 * Auxiliary method that will reduce the OfflineTime pairs.
		 * The list will have the following format: 
		 *  <number of seconds of the last event,
		 *   number of offline seconds before the last event, 
		 *   if the phone is off the network after the last event,
		 *   number of expected offline seconds for all the day>
		 * @param iv1
		 * @param iv2
		 */
		private void reduceOfflineTime(IntermediateValue iv1, IntermediateValue iv2) {
			Integer total = 0;
			// get number of offline seconds seen by iv2.
			int s1 = Integer.parseInt(iv2.getValues().get(1).toString());
			// get number of offline seconds seen by iv1.
			int s2 = Integer.parseInt(iv1.getValues().get(1).toString());
			
			total += s1+s2;
			
			if(iv1.getValues().get(2).equals(Map.NO)) {
				// get number of seconds of iv2's event.
				int s3 = Integer.parseInt(iv2.getValues().get(0).toString());
				// get number of seconds of iv1 event.
				int s4 = Integer.parseInt(iv1.getValues().get(0).toString());
				total += s3-s4;
			}
						 
			iv1.getValues().set(0, iv2.getValues().get(0));
			iv1.getValues().set(1, new Text(total.toString()));
			iv1.getValues().set(2, iv2.getValues().get(2));
			// get the expected number of offline seconds for all the day.
			iv1.getValues().set(3, iv2.getValues().get(3));
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
			return k.getQueryDateId().hashCode() % numReduceTasks;
		}

	}

	/**
	 * Class defining how hadoop should group values (before calling reduce).
	 */
	public static final class GroupingComparator implements RawComparator<IntermediateKey> {
	
		/**
		 * FIXME - inefficient implementation!
		 */
		@Override
		public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
				int arg4, int arg5) {
			return this.compare(
					new IntermediateKey(new String(arg0, arg1, arg2)), 
					new IntermediateKey(new String(arg3, arg4, arg5)));
		}

		@Override
		public int compare(IntermediateKey arg0, IntermediateKey arg1) {
			return   
					arg0.getQuery().compareTo(arg1.getQuery()) == 0 ? 
						arg0.getDate().compareTo(arg1.getDate()) == 0 ?
								arg0.getId().compareTo(arg1.getId()) : 
										arg0.getDate().compareTo(arg1.getDate()) :
												arg0.getQuery().compareTo(arg1.getQuery());
		}
	}
	
	/**
	 * Class defining how hadoop should order keys.
	 */
	public static final class KeyComparator implements RawComparator<IntermediateKey> {

		/**
		 * FIXME - inefficient implementation!
		 */
		@Override
		public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
				int arg4, int arg5) {
			return this.compare(
					new IntermediateKey(new String(arg0, arg1, arg2)), 
					new IntermediateKey(new String(arg3, arg4, arg5)));
		}
		
		@Override
		public int compare(IntermediateKey arg0, IntermediateKey arg1) {
			return   
				arg0.getQuery().compareTo(arg1.getQuery()) == 0 ? 
					arg0.getDate().compareTo(arg1.getDate()) == 0 ?
							arg0.getId().compareTo(arg1.getId()) == 0 ?
									arg0.getTime().compareTo(arg1.getTime()) : 
										arg0.getId().compareTo(arg1.getId()) :
											arg0.getDate().compareTo(arg1.getDate()) :
												arg0.getQuery().compareTo(arg1.getQuery());
		}
	}

	/**
	 * Output formatter. This class will be used to output information into an
	 * SQL table.
	 */
	public static class SQLOutputFormat implements OutputFormat<IntermediateKey, IntermediateValue> {
		
		/**
		 * Access credentials (there credentials are public).
		 */
		private static String USER = "ist167074";
		private static String URL = "jdbc:postgresql://db.ist.utl.pt:5432/ist167074";
		private static String PASS = "eE92Hb41w";
		private static String TABLE = "logs";
		private static Connection conn = null;

		/**
		 * Not needed. Empty implementation.
		 */
		@Override
		public void checkOutputSpecs(FileSystem arg0, JobConf arg1)
				throws IOException {}

		/**
		 * Class defining how to insert information on the database.
		 */
		@Override
		public org.apache.hadoop.mapred.RecordWriter<IntermediateKey, IntermediateValue> getRecordWriter(
				FileSystem arg0, JobConf arg1, String arg2, Progressable arg3)
				throws IOException {
			if (conn == null) 
			{ 
				try {
					Class.forName("org.postgresql.Driver");
					conn = DriverManager.getConnection(URL,USER,PASS); 
				} 
				catch (SQLException e) { throw new IOException(e); } 
				catch (ClassNotFoundException e) { throw new IOException(e); } 
			}
			return new RecordWriter<IntermediateKey, IntermediateValue>() {
				
				@Override
				public void write(IntermediateKey k, IntermediateValue v) throws IOException {
					String date = k.getDate();
					String id=null, number=null, value=null;
					switch(Integer.parseInt(k.getQuery())) { 
					case VISITED_CELLS:
						id = k.getId();
						value = v.getValues().toString();
						break;
					case PRESENT_PHONES:
						String[] idnumber = k.getId().split(":");
						id = k.getId();
						number = idnumber[1];
						value = v.getValues().toString();
						value = value.replaceAll("\\+|,", "");
						value = value.replaceAll("\\-[0-9]*", "");
						break;
					case OFFLINE_TIME:
						id = k.getId();
						Integer tmp = 
								((new Integer(v.getValues().get(3).toString()) + 
								  new Integer(v.getValues().get(1).toString()))/60);
						number =  tmp.toString();
						break;
					}
					
				    try{
				    	String sttmnt = upsert(date, id, number, value);
				    	System.out.println(sttmnt);
				    	conn.prepareStatement(sttmnt).execute();
					    }catch(Exception e){ throw new IOException(e); }			
				}
				
				/**
				 * Method that prepares an UPSERT SQL operation.
				 * This operation may cause race conditions on same scenarios.
				 * However, two notes:
				 * * Only two operations may conflict. All other <K,V> will not
				 * collide.
				 * * The two possible conflicting operations are separate in 
				 * time by intermediate key ordering (so they will arrive here 
				 * later). 
				 * @param date
				 * @param id
				 * @param number
				 * @param value
				 * @return - the sql statement.
				 */
				public String upsert(String date, String id, String number, String value) {
					String update2;
					if(number == null) {
						update2 = " value="+"\'"+value+"\' ";
						number = "0";
					} else if(value == null) {
						update2 = " number="+"\'"+number+"\' ";
						value = "[]";
						
					} else {
						update2 = " value="+"\'"+value+"\', " + " number="+"\'"+number+"\' ";
					}
				    String update = 
				    		" UPDATE "+ TABLE + " SET " + update2 +
				    		" WHERE date=" + "\'" + date + "\'" + " and id=" + "\'"+ id + "\';";
				    
				    String insert = 
				    		" INSERT INTO "+ TABLE + " (date, id, number, value) " +
				    		" SELECT " + "\'" + date +"\'," 
				    				   + "\'" + id + "\',"
				    				   + "\'"+number+"\'," 
				    				   + "\'"+value+"\'" +
				    		" WHERE NOT EXISTS (SELECT 1 FROM "+ TABLE + " WHERE date=" + "\'" + date + "\' and id=\'"+ id + "\');";
				    return update + " " + insert;
				}
				
				/**
				 * Temporary empty implementation.
				 * It could be useful to store a batch of sql updates in order
				 * to commit all of them in just one step.
				 */
				@Override
				public void close(Reporter arg0) throws IOException {}
			};
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

		conf.setOutputKeyClass(IntermediateKey.class);
		conf.setOutputValueClass(IntermediateValue.class);

		conf.setOutputKeyComparatorClass(KeyComparator.class);
		conf.setOutputValueGroupingComparator(GroupingComparator.class);
		conf.setPartitionerClass(Partition.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputFormat(SQLOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
}
