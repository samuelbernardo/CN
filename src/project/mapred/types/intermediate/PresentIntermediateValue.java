package project.mapred.types.intermediate;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;

import project.mapred.Runner.Map;

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
	 *  - [+/-] phone
	 */
	@Override
	public IntermediateValue merge(IntermediateValue iv) {
		for (Iterator<Text> i = iv.getValues().iterator(); i.hasNext();) {
			// Algorithm explanation:
			// if contains -> continue
			// else
			//   -> flip +/-
			//   -> if contains -> nothing
			//      else -> if tmp starts with + -> remove
			//           -> if tmp starts with - -> add
			Text tmp = i.next();
			if (this.values.contains(tmp)) { continue; }
			else {
				tmp.set((""+(tmp.charAt(0) == Map.ENTER ? Map.LEAVE : Map.ENTER)).getBytes(), 0, 2);
				if (this.values.contains(tmp)) {
					@SuppressWarnings("unused")
					boolean b = tmp.charAt(0) == '+' ? 
					  this.values.remove(tmp) : this.values.add(tmp);
				}
			}			
		}
		return this;
	}
}