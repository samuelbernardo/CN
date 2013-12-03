package project.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import project.mapred.types.intermediate.IntermediateKey;
import project.mapred.types.intermediate.IntermediateValue;

public class CombinerImpl {
	
	public static class Combine 
	extends MapReduceBase 
	implements Reducer<IntermediateKey, IntermediateValue, IntermediateKey, IntermediateValue> {
		
		/**
		 * Combine implementation.
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
			IntermediateValue fv = new IntermediateValue(it.next().getValues());
			 // Only combine the PRESENT_PHONES
			 // The received list uses the following format: <eventTime, [+|-]phone>
			 // The produced list uses the following format: <[+|-]phone,...>
			switch (Integer.parseInt(k.getQuery())) {
			case Runner.VISITED_CELLS:
				// TODO: read database id=k.getId(), data=k.getDate() ->value
				break;
			case Runner.PRESENT_PHONES:
				String firstEventTime = fv.getValues().get(0).toString();
				List<Text> phones = new ArrayList<Text>();
				phones.add(fv.getValues().get(1));
				for(IntermediateValue value = null; it.hasNext(); ) {
					value = it.next();
					Text phone = value.getValues().get(1);
					// if contains, nothing to do.
					if(phones.contains(phone)) { continue; }
					else {
						// change + to - or - to + 
						// FIXME: inefficient code kills trees!
						Text iphone = new Text(phone);
						if(phone.charAt(0) == '+') { iphone.set(phone.toString().replace('+', '-')); }
						else { iphone.set(phone.toString().replace('-', '+')); }
						// if contains the inverse, nothing to do.
						if (phones.contains(iphone)) { continue; }
						// if doen't contain, insert
						else { phones.add(phone); }
					}
				}
				// prepare pair to be collected.
				k = new IntermediateKey(k.getQuery(), k.getDate(), firstEventTime, k.getId());
				fv.setValues(phones);
				break;
			case Runner.OFFLINE_TIME:
				// TODO: read database id=k.getId(), data=k.getDate() ->number
				break;
			}
			output.collect(k, fv);
		}
	}
}

