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
	 * Just to remember,
	 * values (List):
	 *  - <visited_cell_1, visited_cell_2, ...>.
	 */
	@Override
	public IntermediateValue merge(IntermediateValue iv) {
		this.values.addAll(iv.getValues());
		return this;
	}
}