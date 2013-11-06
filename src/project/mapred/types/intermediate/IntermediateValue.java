package project.mapred.types.intermediate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


/**
 * Class that represents the intermediate values.
 * NOTE: it seems that hadoop doen't instantiate multiple objects when calling 
 * reduce. Therefore, the readFields must do all the initialization. 
 */
public class IntermediateValue 
implements WritableComparable<IntermediateValue> {

	/**
	 * This is where all the values are stored.
	 */
	protected List<Text> values = null;
	
	public static final String SEPARATOR = ", ";

	/**
	 * Constructors.
	 */
	public IntermediateValue(List<Text> values) { this.values = values; }
	public IntermediateValue() {}

	/**
	 * Method that serializes the class fields.
	 * @param out - where fields will be written.
	 */
	@Override
	public void write(DataOutput out) throws IOException 
	{ Text.writeString(out, this.toString()); }

	/**
	 * Method that deserializes the class fields.
	 * @param in - where fields will be read from.
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		String serialize = Text.readString(in);
		String[] values = serialize.trim().split(SEPARATOR);
		int counter = 0;
		this.values = new ArrayList<Text>();
		for(; counter < values.length; counter++)
		{ this.values.add(new Text(values[counter])); }
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
	public int compareTo(IntermediateValue o) 
	{ return this.toString().compareTo(o.toString()); }
	
	/**
	 * toString implementation.
	 * It cuts off the '[' and ']' given by the ArrayList toString.
	 * @return
	 */
	@Override
	public String toString() {
		String output = this.values.toString();
		int len = output.length();
		return output.substring(1, len-1); 
	}
}
