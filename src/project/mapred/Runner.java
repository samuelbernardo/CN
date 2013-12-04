package project.mapred;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

import project.mapred.types.intermediate.*;

/**
 * Class that holds the job configuration classess.
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
		 * Helper to get a connection.
		 * @return - an sql connection.
		 * @throws IOException - if something goes wrong creating the connection.
		 */
		public static Connection getSQLConnection() throws IOException {
			if (conn == null) { 
				try {
					Class.forName("org.postgresql.Driver");
					conn = DriverManager.getConnection(URL,USER,PASS); 
				} 
				catch (SQLException e) { throw new IOException(e); } 
				catch (ClassNotFoundException e) { throw new IOException(e); } 
			}
			return conn;
		}
		

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
						Integer tmp = new Integer(v.getValues().get(0).toString())/60;
						number =  tmp.toString();
						break;
					}
					
				    try{
				    	String sttmnt = upsert(date, id, number, value);
				    	getSQLConnection().prepareStatement(sttmnt).execute();
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

		conf.setMapperClass(MapperImpl.Map.class);
		conf.setCombinerClass(CombinerImpl.Combine.class);
		conf.setReducerClass(ReducerImpl.Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputFormat(SQLOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
}
