package project.mapred.types.intermediate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


/**
 * 
 */
public class IntermediateKey implements WritableComparable<IntermediateKey> {
    /**
     * TODO
     */
    private Text date;
    /**
     * TODO
     */
    private Text time;
    
    /**
     * TODO
     */
    private Text id;

    /**
     * Constructor.
     */
    public IntermediateKey(Text date, Text time, Text id) {
    	this.date = date;
    	this.time = time;
        this.id = id;
    }

    /**
     * Getters.
     */
    public Text getDate() { return this.date; }
    public Text getTime() {return this.time; }
    public Text getId() { return this.id; } 

    /**
     * Method that serializes the class fields.
     * @param out - where fields will be written.
     */
    @Override
    public void write(DataOutput out) throws IOException {
    	this.date.write(out);
    	this.time.write(out);
    	this.id.write(out); 
    }
  
    /**
     * Method that deserializes the class fields.
     * @param in - where fields will be read from.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
	    this.date.readFields(in);
	    this.time.readFields(in);
	    this.id.readFields(in);
    }
  
    /**
     * Method that compares two ReduceKey objects.
     * It starts matching the ids and then the dates. 
     * @return -1, 0 or 1
     */
    @Override
    public int compareTo(IntermediateKey o){
      return 
        this.date.compareTo(o.getDate()) == 0 ? 
          this.time.compareTo(o.getTime()) == 0 ? 
            this.id.compareTo(o.getId()) : 
              this.time.compareTo(o.getTime()) : 
                this.date.compareTo(o.getDate()); 
    }
}
