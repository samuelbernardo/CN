package project.mapred.types.intermediate;

import org.apache.hadoop.io.Text;

/**
 * 
 */
public class IntermediateKey extends Text {

	public static final String SEPARATOR = ";";
	public static final int DATE_BEG = 0;
	public static final int DATE_END = 9;
	public static final int TIME_BEG = 11;
	public static final int TIME_END = 18;
	public static final int ID_BEG = 20;

	/**
	 * Constructor.
	 */
	public IntermediateKey(String date, String time, String id) {
		super(date + SEPARATOR + time + SEPARATOR + id);
	}

	/**
	 * Getters.
	 */
	public String getDate() 
	{ return this.toString().substring(DATE_BEG, DATE_END+1); }
	public String getTime() 
	{return this.toString().substring(TIME_BEG, TIME_END+1); }
	public String getId() 
	{ return this.toString().substring(ID_BEG); }
	public String getDateId()
	{ return this.toString().substring(DATE_BEG, TIME_END+1); }
}
