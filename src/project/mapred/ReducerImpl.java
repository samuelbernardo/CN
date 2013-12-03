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

import project.mapred.MapperImpl.Map;
import project.mapred.types.intermediate.IntermediateKey;
import project.mapred.types.intermediate.IntermediateValue;

public class ReducerImpl {
	
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
			IntermediateValue fv = new IntermediateValue(it.next().getValues());
			switch (Integer.parseInt(k.getQuery())) {
			/**  
			 * The received list uses the following format: <time;Cx>
			 * The produced list uses the following format: <time;Cx1,...>
			 */
			case Runner.VISITED_CELLS:
				String lastCell = fv.getValues().get(0).toString().split(";")[1];
				List<Text> visited = new ArrayList<Text>();
				visited.add(new Text(lastCell));
				for(IntermediateValue value = null; it.hasNext();) {
					value = it.next();
					String cell = value.getValues().get(0).toString().split(";")[1];
					// ignore replicated events
					if(lastCell.equals(cell)) { continue; }
					lastCell = cell;
					visited.add(value.getValues().get(0));
				}
				fv.setValues(visited);
				break;
			/** 
			 * The received list uses the following format: <[+|-]phone,...>
			 * The produced list uses the following format: <[+|-]phone,...>
			 */
			case Runner.PRESENT_PHONES:
				List<Text> phones = fv.getValues();
				for(IntermediateValue value = null; it.hasNext(); ) {
					value = it.next();
					Text phone = value.getValues().get(0);
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
				// Removes phones with initial '+' (the ones that entered after)
				// Removes initial '-' from the other
				for(int i = 0; i < phones.size(); i++) { 
					String phone = phones.get(i).toString();
					if(phone.startsWith("+")) { phones.remove(i--); } 
					else { phones.set(i, new Text(phone.substring(1))); }
				}
				break;
			/**  
			 * The received list uses the following format: <[E|L],eventTime>
			 * The produced list uses the following format: <onlineTime>
			 */
			case Runner.OFFLINE_TIME:
				String lastState = fv.getValues().get(0).toString();
				Integer onlineTime = 0;
				Integer lastEventTime = Integer.parseInt(fv.getValues().get(1).toString());
				// if so, it means that the phone has been offline for the all day.
				if(lastState.equals(Map.LEAVE))	
				{ onlineTime = lastEventTime; }
				// reduce left events
				for(IntermediateValue value = null; it.hasNext(); ) {
					value = it.next();
					String state = value.getValues().get(0).toString();
					Integer eventTime = Integer.parseInt(value.getValues().get(1).toString());
					// ignore replicated events
					if(state.equals(lastState)) { continue; }
					// means that in the mean while, the phone was online
					if(state.equals(Map.LEAVE)) 
					{ onlineTime += (eventTime - lastEventTime); }
					// save last state
					lastState = state;
					lastEventTime = eventTime;
				}
				// Fix the online time for the rest of the day.
				onlineTime += lastState.equals(Map.ENTER) ? Map.SECONDS_IN_DAY - lastEventTime : 0;
				List<Text> online = new ArrayList<Text>();
				online.add(new Text(new Text(onlineTime.toString())));
				fv.setValues(online);
				break;
			}
			output.collect(k, fv);
		}
	}
}
