import java.util.List;

import org.apache.hadoop.io.Text;

/**
 * TODO
 */
public class OffIntermediateValue extends IntermediateValue{

	/**
	 * Constructor.
	 */
	public OffIntermediateValue(List<Text> values) { super(values);}

	/**
	 * See base class for doc.
	 */
	@Override
	public IntermediateValue merge(IntermediateValue iv) {
		// TODO Auto-generated method stub
		return iv;
	}
}