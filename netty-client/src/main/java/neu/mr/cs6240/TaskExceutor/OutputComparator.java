package neu.mr.cs6240.TaskExceutor;

import java.util.Comparator;

import org.apache.log4j.Logger;

/**
 * Comparator used on OutputData to sort the files
 * 
 * @author smitha
 *
 */
public class OutputComparator implements Comparator<OutputData> {

	final Logger logger = Logger.getLogger(OutputComparator.class);

	@Override
	public int compare(OutputData o1, OutputData o2) {
		return o1.getDryBulbTemp().compareTo(o2.getDryBulbTemp());
	}
}
