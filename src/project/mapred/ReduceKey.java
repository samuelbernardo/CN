import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.WritableComparable;


/**
 * 
 */
public class ReduceKey implements WritableComparable<ReduceKey> {
  /**
   * Identifier can represent a phone or a cell.
   */
  private String id;
  /**
   * This class holds the date and time values.
   */
  private Date date;

  /**
   * Constructor.
   */
  public ReduceKey(String id, Date date) {
    this.id = id;
    this.date = date;
  }

  /**
   * Getters.
   */
  public Date getDate() { return this.date; }
  public String getId() { return this.id; } 

  /**
   * Method that serializes the class fields.
   * @param out - where fields will be written.
   */
  public void write(DataOutput out) throws IOException {
	  out.writeChars(this.id);
	  out.writeLong(this.date.getTime()); 
  }
  
  /**
   * Method that deserializes the class fields.
   * @param in - where fields will be read from.
   */
  public void readFields(DataInput in) throws IOException {
	  this.id = in.readLine();
	  this.date = new Date(in.readLong());
  }
  
  /**
   * Method that compares two ReduceKey objects.
   * It starts matching the ids and then the dates. 
   * @return -1, 0 or 1
   */
  public int compareTo(ReduceKey o){
    return 
      (id.compareTo(o.getId()) == 0 ? this.date.compareTo(o.getDate()) : 
    	                              id.compareTo(o.getId())); 
  }

}
