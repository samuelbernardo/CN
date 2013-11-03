package project.mapred.types.intermediate;

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

/**
 * Class representing the intermediate key.
 * This is a generic key that shall be used by all intermediate pairs.
 */
public class IntermediateKey extends Text {

	public static final String SEPARATOR = ";";
	public static final int SEPARATOR_SIZE = SEPARATOR.getBytes().length;
	public static final int TIME_SIZE = 8;
	private String query;
	private String date;
	private String time;
	private String id;

	/**
	 * Constructors.
	 * Format example: 
	 * "2;2013-10-09;960123123;17:54:01"
	 * "event:date:id:time"
	 */
	public IntermediateKey(String query, String date, String time, String id) {
		super(query + SEPARATOR +date + SEPARATOR + id + SEPARATOR + time);
		this.date = date;
		this.query = query;
		this.time = time;
		this.id = id;
	}
	public IntermediateKey() {}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		String[] key = this.toString().split(SEPARATOR);
		this.query = key[0];
		this.date = key[1];
		this.id = key[2];
		this.time = key[3];
	}
	
	/**
	 * Getters.
	 */
	public String getDate() 
	{ return this.date; }
	public String getTime() 
	{ return this.time; }
	public String getId() 
	{ return this.id; }
	public String getDateId()
	{ return new String(this.getDate()+";"+this.getId()); }
	public String getQuery()
	{ return this.query; }
}
