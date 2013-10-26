package project.mapred;

import java.util.List;

import org.apache.hadoop.io.Text;

/**
 * TODO
 */
public class PresentIntermediateValue extends IntermediateValue{

	/**
	 * Constructor.
	 */
	public PresentIntermediateValue(List<Text> values) { super(values);}

	/**
	 * See base class for doc.
	 * Just to remember,
	 * values (List):
	 *  - number of seconds for the last event;
	 *  - [Y/N]*12
	 */
	@Override
	public IntermediateValue merge(IntermediateValue iv) {
		// TODO Auto-generated method stub
		// copy the iv list from its hour until de end.
		//  
		return this;
	}
}