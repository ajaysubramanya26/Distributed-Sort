package neu.mr.cs6240.utils;

/**
 * This class holds the client address the range assigned to it and the count of that range ,i.e., bucket
 * @author prasad memane
 * @author swapnil mahajan
 */
public class Bucket implements Comparable<Bucket> {
	
	private String clientAddress;
	private String range;
	private String count;
	
	public Bucket(String clientAddress, String range, String count) {	
		this.clientAddress = clientAddress;
		this.range = range;
		this.count = count;
	}

	public String getRange() {
		return range;
	}

	public void setRange(String range) {
		this.range = range;
	}

	public String getCount() {
		return count;
	}

	public void setCount(String count) {
		this.count = count;
	}

	public String getClientAddress() {
		return clientAddress;
	}

	public void setClientAddress(String clientAddress) {
		this.clientAddress = clientAddress;
	}

	@Override
	public int compareTo(Bucket o) {
		int result = this.count.compareTo(o.count);
		return result;
	}

	@Override
	public String toString() {
		return "Bucket [clientAddress=" + clientAddress + ", range=" + range
				+ ", count=" + count + "]";
	}
	
}
