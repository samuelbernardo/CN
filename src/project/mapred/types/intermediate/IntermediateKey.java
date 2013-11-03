package project.mapred.types.intermediate;

import org.apache.hadoop.io.Text;

/**
 * Class representing the intermediate key.
 * This is a generic key that shall be used by all intermediate pairs.
 */
public class IntermediateKey extends Text {

	public static final String SEPARATOR = ";";
	public static final int TIME_SIZE = 8;
	private int queryOff;
	private int queryLen;
	private int dateOff;
	private int dateLen;
	private int timeOff;
	private int timeLen;
	private int idOff;
	private int idLen;

	/**
	 * Constructor.
	 * Format example: 
	 * "2:2013-10-09;960123123;17:54:01"
	 * "event:date:id:time"
	 */
	public IntermediateKey(String event, String date, String time, String id) {
		super(event + SEPARATOR +date + SEPARATOR + id + SEPARATOR + time);
		this.queryOff = 0;
		this.queryLen = event.length();
		this.dateOff = this.queryOff + this.queryLen + SEPARATOR.length();
		this.dateLen = date.length();
		this.idOff = this.dateOff + this.dateLen + SEPARATOR.length();
		this.idLen = id.length();
		this.timeOff = this.idOff + this.idLen + SEPARATOR.length();
		this.timeLen = time.length();
	}

	/**
	 * Getters.
	 */
	public String getDate() 
	{ return new String(this.getBytes(), this.dateOff, this.dateLen); }
	public String getTime() 
	{ return new String(this.getBytes(), this.timeOff, this.timeLen); }
	public String getId() 
	{ return new String(this.getBytes(), this.idOff, this.idLen); }
	public String getDateId()
	{ return new String(this.getBytes(), this.dateOff, this.dateLen + SEPARATOR.length() + this.idLen); }
	public String getQuery()
	{ return new String(this.getBytes(), this.queryOff, this.queryLen); }
}
