package project.mapred.types.intermediate;

import org.apache.hadoop.io.Text;

/**
 * Class representing the intermediate key.
 * This is a generic key that shall be used by all intermediate pairs.
 */
public class IntermediateKey extends Text {

	public static final String SEPARATOR = ";";
	public static final int TIME_SIZE = 8;
	public int dateBeg;
	public int dateEnd;
	public int timeBeg;
	public int timeEnd;
	public int idBeg;
	public int idEnd;

	/**
	 * Constructor.
	 * Format example: "2013-10-09;960123123;17:54.01"
	 */
	public IntermediateKey(String date, String time, String id) {
		super(date + SEPARATOR + id + SEPARATOR + time);
		this.dateBeg = 0;
		this.dateEnd = 9;
		this.idBeg = 11;
		this.idEnd = this.idBeg + id.length();
		this.timeBeg = this.idEnd + 2;
		this.timeEnd = this.timeBeg + 7;
	}

	/**
	 * Getters.
	 */
	public String getDate() 
	{ return new String(this.getBytes(), this.dateBeg, this.dateEnd - this.dateBeg); }
	public String getTime() 
	{ return new String(this.getBytes(), this.timeBeg, this.timeEnd - this.timeBeg); }
	public String getId() 
	{ return new String(this.getBytes(), this.idBeg, this.idEnd - this.idBeg); }
	public String getDateId()
	{ return new String(this.getBytes(), this.dateBeg, this.idEnd - this.dateBeg); }
}
