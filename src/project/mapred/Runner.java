
import java.io.IOException;
import java.util.*; 	

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
 	
 	public class Runner {
 	
 	   public static class Map 
 	    extends MapReduceBase 
 	    implements Mapper<LongWritable, Text, IntermediateKey, IntermediateValue> {
 		   
 		   /**
 		    * Events to process
 		    */
 		   private static final int PHONE_JOINS_NETWORK = 4; 
 		   private static final int PHONE_LEAVES_NETWORK = 5; 
 		   private static final int PHONE_JOINS_CELL = 2; 
 		   private static final int PHONE_LEAVES_CELL = 3;

 	
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

 	       switch (Integer.parseInt(line[3])) {
 	       case PHONE_JOINS_NETWORK:
 	    	    List<String> list1 = new ArrayList<String>(); 
 	    	    list1.add("0");
 	    	    list1.add("Y");
 	    	    output.collect(
    	    	  new IntermediateKey(this.stringToDate(line[1]), line[4]), 
 	    	    	  new IntermediateValue(this.stringToSeconds(line[2]), list1));
 	    	   break;
 	       case PHONE_LEAVES_NETWORK:
	    	    List<String> list2 = new ArrayList<String>(); 
	    	    list2.add("0");
	    	    list2.add("N");
	    	    output.collect(
   	    	  new IntermediateKey(this.stringToDate(line[1]), line[4]), 
	    	    	  new IntermediateValue(this.stringToSeconds(line[2]), list2)); 	    	   
	    	    break;
 	       case PHONE_JOINS_CELL:
	    	    List<String> list3 = new ArrayList<String>(); 
	    	    list3.add("N");
	    	    list3.add("S");
	    	    output.collect(
  	    	      new IntermediateKey(this.stringToDate(line[1]), line[0]+line[4]), 
	    	    	  new IntermediateValue(this.stringToSeconds(line[2]), list3));
	    	    List<String> list4 = new ArrayList<String>(); 
	    	    list4.add(line[0]);
	    	    output.collect(
   	    	      new IntermediateKey(this.stringToDate(line[1]), line[4]), 
	    	    	  new IntermediateValue(this.stringToSeconds(line[2]), list4));
 	    	   break;
 	       case PHONE_LEAVES_CELL:
	    	    List<String> list5 = new ArrayList<String>(); 
	    	    list5.add("S");
	    	    list5.add("N");
	    	    output.collect(
 	    	      new IntermediateKey(this.stringToDate(line[1]), line[0]+line[4]), 
	    	    	  new IntermediateValue(this.stringToSeconds(line[2]), list5));
 	    	   break;
 	       }
 	     }
 	     
 	     /**
 	      * TODO
 	      * @param date
 	      */
 	     public Date stringToDate(String date) {
 	    	 return null;
 	     }
 	     
 	     /**
 	      * 
 	      * @param time
 	      * @return
 	      */
 	     public int stringToSeconds(String time) {
 	    	 return 0;
 	     }
 	     
 	   }
 	
 	   public static class Reduce 
 	    extends MapReduceBase 
 	    implements Reducer<Text, IntWritable, Text, IntWritable> {
 	     public void reduce(
 	    		 Text key, 
 	    		 Iterator<IntWritable> values, 
 	    		 OutputCollector<Text, IntWritable> output, 
 	    		 Reporter reporter) throws IOException {
 	       int sum = 0;
 	       while (values.hasNext()) {
 	         sum += values.next().get();
 	       }
 	       output.collect(key, new IntWritable(sum));
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
