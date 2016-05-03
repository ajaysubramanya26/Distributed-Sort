package neu.mr.cs6240.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * This class maintains the list of the files to be assigned to a client and its total size
 * @author swapnil mahajan
 *
 */
public class TransferFillesTracker  implements Comparable<TransferFillesTracker>{
	private List<String> files;
	private Long size;
	
	public TransferFillesTracker(Long size) {
		this.files = new ArrayList<>();
		this.size = size;
	}
	
	public List<String> getFiles() {
		return files;
	}
	public void setFiles(List<String> files) {
		this.files = files;
	}
	public Long getSize() {
		return size;
	}
	public void setSize(Long size) {
		this.size = size;
	}

	@Override
	public int compareTo(TransferFillesTracker o) {
		return this.size.compareTo(o.getSize());
	}
	
	
}
