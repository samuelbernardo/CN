package project.mapred.types.intermediate;

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
		// WARNING: there could be an issue here. If in the first hour of a day,
		// the phone leaves the cell before pinging it, we will not record that
		// the phone was in that cell.
		// UGLY SOLUTION: see if the disconnection happens within the first 
		// hour and reproduce a ping.
		this.values.addAll(iv.getValues());
		return this;
	}
}