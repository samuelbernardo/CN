package project.mapred;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
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
		 * TODO 
		 */
		private static HashMap<String,String> cache_offline = new HashMap<String,String>();
		private static HashMap<String,String> cache_visited = new HashMap<String,String>();
			
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
				visited.add(new Text(fv.getValues().get(0)));
				for(IntermediateValue value = null; it.hasNext();) {
					value = it.next();
					String cell = value.getValues().get(0).toString().split(";")[1];
					// ignore replicated events
					if(lastCell.equals(cell)) { continue; }
					lastCell = cell;
					visited.add(value.getValues().get(0));
				}
				// get info stored on the DB
				String visited_db = getSQLRecord(cache_visited, k.getDate(), k.getId());
				String[] dbv = visited_db == null ? new String[0] : visited_db.split(" ");
				ArrayList<Text> dbl = new ArrayList<Text>();
				// Create a new list of texts
				for(String s : dbv) { dbl.add(new Text(s)); }
				// merge two sorted lists
				for(int i = 0; i < visited.size() && dbl.size() > 0; i++) {
					// remove duplicates
					if(visited.get(i).compareTo(dbl.get(0)) == 0)
					{ dbl.remove(0); }
					else if(visited.get(i).compareTo(dbl.get(0)) > 0) 
					{ visited.add(i, dbl.remove(0)); }
				}
				// merge the rest
				visited.addAll(dbl);
				
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
				
				// get info stored on the DB
				String offline_db_str = getSQLRecord(cache_offline, k.getDate(), k.getId());
				Integer offline_db = offline_db_str == null ? 0 : offline_db_str.equals("0") ? 0 : Integer.parseInt(offline_db_str);
				if(offline_db != 0) 
				{ onlineTime += offline_db; }
				// Fix the online time for the rest of the day if it is the 
				// first record.
				else 
				{ onlineTime += lastState.equals(Map.ENTER) ? Map.SECONDS_IN_DAY - lastEventTime : 0;}
				List<Text> online = new ArrayList<Text>();
				online.add(new Text(onlineTime.toString()));
				fv.setValues(online);
				break;
			}
			output.collect(k, fv);
		}		
	
		/**
		 * TODO
		 * @param data
		 * @param id
		 * @return
		 */
		public String getSQLRecord(HashMap<String,String> cache, String date, String id) {
			if(cache.containsKey(date+id)) { return cache.get(date+id); }
			else {
				ResultSet rs = null;
				try {
					Connection conn = Runner.SQLOutputFormat.getSQLConnection();
					Statement stmt = conn.createStatement(
							ResultSet.TYPE_FORWARD_ONLY, 
							ResultSet.CONCUR_READ_ONLY);
					rs = stmt.executeQuery("SELECT number, value FROM logs WHERE date=\'"+date+"\' and id=\'"+id+"\'");
					if(rs.next()) { 
						cache_offline.put(date+id, rs.getString("number"));
						cache_visited.put(date+id, rs.getString("value"));
					}
				} 
				catch (SQLException e) { e.printStackTrace(); }
				catch (IOException e) { e.printStackTrace(); }
				// just close the results.
				finally { 
					if(rs != null) {
						try { rs.close(); }
						// this should not happen
						catch (SQLException e) { e.printStackTrace(); } 
					} 
				}
				// if some error happened or if there is no registry in the DB.
				return cache.get(date+id);
			}
		}
	}
}
