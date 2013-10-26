package project.mapred;

import java.io.IOException;
import java.util.*; 	

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
 	
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
 		   
 		   /**
 		    * Constants used for values.
 		    */
 		   public static final String YES = "Y";
 		   public static final String NO = "N";
 		  public static final String ZERO = "0";
 		  public static final int SECONDS_IN_DAY = 60*60*60;

 	
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
 	    	    this.collect(output, line[0], line[2], line[4], iv);
 	    	    break;
 	       case PHONE_LEAVES_NETWORK: 
 	    	    list.add(new Text(new Integer(
 	    	      this.getNumberSeconds(line[2])).toString()));
	    	    list.add(new Text(Map.ZERO));
	    	    list.add(new Text(Map.NO));
	    	    list.add(new Text(new Integer(
	    	      Map.SECONDS_IN_DAY - this.getNumberSeconds(line[2])).toString()));
	    	    iv = new OffIntermediateValue(list);
	    	    this.collect(output, line[0], line[2], line[4], iv); 	    	   
	    	    break;
 	       case PHONE_JOINS_CELL:
	    	    list.add(new Text(Map.NO));
	    	    list.add(new Text(Map.YES));
	    	    iv = new PresentIntermediateValue(list);
	    	    this.collect(output, line[0], line[2], line[0]+line[4], iv);
	    	    list = new ArrayList<Text>(); 
	    	    list.add(new Text(line[0]));
	    	    iv = new CellsIntermediateValue(list);
	    	    this.collect(output, line[0], line[2], line[4], iv);
 	    	   break;
 	       case PHONE_LEAVES_CELL:
	    	    list.add(new Text(Map.YES));
	    	    list.add(new Text(Map.NO));
	    	    iv = new PresentIntermediateValue(list);
	    	    this.collect(output, line[0], line[2], line[0]+line[4], iv);
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
 	    	 output.collect(
 	    			 new IntermediateKey(
 	    					 new Text(date), new Text(time), new Text(id)), 
 	    			 value);
 	     }
 	     
 	 	   /**
 	 	    * 
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
 		    * This map will keep the objects already in the collector (through 
 		    * all calls to reduce function).
 		    */
 		   AbstractMap<String, IntermediateValue> htable = 
 				   new HashMap<String, IntermediateValue>();

 		   /**
 		    * TODO
 		    */
 		   public void reduce(
 	    		IntermediateKey key, 
 	    		 Iterator<IntermediateValue> it, 
 	    		 OutputCollector<IntermediateKey, IntermediateValue> output, 
 	    		 Reporter reporter) throws IOException {

 			 String hkey = key.getDate().toString()+key.getId().toString();
 	    	 while(it.hasNext()) {
 	    		 if(this.htable.containsKey(hkey)) 
 	    		 { this.htable.get(hkey).merge(it.next()); }
 	    		 else {
 	    			 // TODO: day transition
 	    			 this.htable.put(hkey, it.next());
 	    			 output.collect(key, this.htable.get(hkey));
 	    	     }
 	    	 }
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
