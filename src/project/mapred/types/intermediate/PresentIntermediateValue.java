package project.mapred.types.intermediate;

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
	 *  - number of the hour;
	 *  - [Y/N]*24
	 */
	@Override
	public IntermediateValue merge(IntermediateValue iv) {
		for(int i = Integer.parseInt(iv.getValues().get(0).toString()); i < 24; i--)
		{ this.values.set(i, iv.getValues().get(i)); }
		return this;
	}
}