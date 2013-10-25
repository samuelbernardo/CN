package project.mapred;

import java.util.List;

import org.apache.hadoop.io.Text;

/**
 * TODO
 */
public class CellsIntermediateValue extends IntermediateValue{

	/**
	 * Constructor.
	 */
	public CellsIntermediateValue(List<Text> values) { super(values);}

	/**
	 * See base class for doc.
	 */
	@Override
	public IntermediateValue merge(IntermediateValue iv) {
		this.values.addAll(iv.getValues());
		return iv;
	}
}