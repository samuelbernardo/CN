package project.mapred.types.intermediate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Class that represents the intermediate values. 
 */
public abstract class IntermediateValue 
implements WritableComparable<IntermediateValue> {

	/**
	 * This is where all the values are stored.
	 */
	protected List<Text> values;

	/**
	 * Constructor.
	 */
	public IntermediateValue(List<Text> values) { this.values = values; }

	/**
	 * Method that serializes the class fields.
	 * @param out - where fields will be written.
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		Iterator<Text> iterator = this.values.iterator();
		out.writeInt(this.values.size());
		while(iterator.hasNext()) { iterator.next().write(out); }
	}

	/**
	 * Method that deserializes the class fields.
	 * @param in - where fields will be read from.
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		int counter;
		Text tmp;
		for(tmp = new Text(), counter = in.readInt(); 
			counter > 0; 
			tmp.readFields(in), counter--, this.values.add(tmp));
	}

	/**
	 * Getters
	 */
	public List<Text> getValues() { return this.values; }

	/**
	 * Compares two objects of the same class.
	 * @return always 0 (we are not sorting values).
	 */
	@Override
	public int compareTo(IntermediateValue o) { return 0; }

	/**
	 * This method merges two IntermediateValues into one. The local object
	 * stores the resultant merged object.
	 * @param iv 
	 * @return - the merged object.
	 */
	public abstract IntermediateValue merge(IntermediateValue iv);
}
