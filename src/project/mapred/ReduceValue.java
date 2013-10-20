import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 *
 */
public class ReduceValue implements WritableComparable<ReduceValue>{
	
	/**
	 * Valid events.
	 */
	public static enum Event {
		PHONE_JOINS_CELL,
		PHONE_LEAVES_CELL,
		PHONE_JOINS_NETWORK,
		PHONE_LEAVES_NETWORK
	}
	
    /**
     * Identifier can represent a phone or a cell.
     */
    private String id;
    /**
     * Integer representing the event.
     */
    private Event event;
    
    /**
     * Constructor.
     */
    public ReduceValue(String id, Event event) {
    	this.id = id;
    	this.event = event;
    }

    /**
     * Method that serializes the class fields.
     * @param out - where fields will be written.
     */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeChars(this.id);
		out.writeInt(this.event.ordinal());
	}
	
    /**
     * Method that deserializes the class fields.
     * @param in - where fields will be read from.
     */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.id = in.readLine();
		this.event = Event.values()[in.readInt()];		
	}

	@Override
	public int compareTo(ReduceValue o) {
		// TODO Auto-generated method stub
		return 0;
	}
}
