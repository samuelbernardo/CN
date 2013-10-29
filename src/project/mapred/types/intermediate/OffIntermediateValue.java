package project.mapred.types.intermediate;

import java.util.List;

import org.apache.hadoop.io.Text;

import project.mapred.Runner.Map;

/**
 * TODO
 */
public class OffIntermediateValue extends IntermediateValue{

	/**
	 * Constructor.
	 */
	public OffIntermediateValue(List<Text> values) { super(values);}

	/**
	 * See base class for doc.
	 * Just to remember,
	 * values (List):
	 *  - number of seconds for the last event;
	 *  - number of offline seconds before the last event; 
	 *  - if the phone is off the network after the last event;
	 *  - number of expected offline seconds for all the day.
	 */
	@Override
	public IntermediateValue merge(IntermediateValue iv) {
		Integer total = 0;
		// get number of offline seconds seen by iv.
		int s1 = Integer.parseInt(iv.getValues().get(1).toString());
		// get number of offline seconds seen by us.
		int s2 = Integer.parseInt(this.getValues().get(1).toString());
		
		total += s1+s2;
		
		if(this.values.get(2).equals(Map.NO)) {
			// get number of seconds of iv's event.
			int s3 = Integer.parseInt(iv.getValues().get(0).toString());
			// get number of seconds of our event.
			int s4 = Integer.parseInt(this.getValues().get(0).toString());
			total += s3-s4;
		}
		
		// get the expected number of offline seconds for all the day.
		int s5 = Integer.parseInt(iv.getValues().get(3).toString());
		 
		this.values.set(0, iv.getValues().get(0));
		this.values.set(1, new Text(total.toString()));
		this.values.set(2, iv.getValues().get(2));
		this.values.set(3, new Text(new Integer(total + s5).toString()));
		
		return this;
	}
}
