import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

/**
 * TODO
 */
public class IntermediateValue implements WritableComparable<IntermediateValue>{
	
    /**
     * TODO
     */
    private List<String> values;
    
    /**
     * TODO
     */
    private int number;
    
    /**
     * Constructor.
     */
    public IntermediateValue(int number, List<String> values) {
    	this.values = values;
    	this.number = number;
    }

    /**
     * Method that serializes the class fields.
     * @param out - where fields will be written.
     */
	@Override
	public void write(DataOutput out) throws IOException {
		Iterator<String> iterator = this.values.iterator();
		
		out.writeInt(this.number);
		out.writeInt(this.values.size());
		
		while(iterator.hasNext()) {
			out.writeBytes(iterator.next()+'\n');
		}
	}
	
    /**
     * Method that deserializes the class fields.
     * @param in - where fields will be read from.
     */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.number = in.readInt();
		
		for(int counter = in.readInt(); counter > 0; counter--) {
			this.values.add(in.readLine());
		}
	}
	
	/**
	 * Getters
	 */
	public List<String> getValues() {
		return this.values;
	}
	public int getNumber() {
		return this.number;
	}

	/**
	 * Compares two objects of the same class.
	 * @return -1,0,1
	 */
	@Override
	public int compareTo(IntermediateValue o) {
		return this.number == o.getNumber()  ? 
				0 : 
				(this.number < o.getNumber() ? -1 : 1);
	}
}
