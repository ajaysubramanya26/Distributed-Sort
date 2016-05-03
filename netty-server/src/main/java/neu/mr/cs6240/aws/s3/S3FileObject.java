package neu.mr.cs6240.aws.s3;

/**
 * This is a POJO class for the S3 Files which holds file name and size
 * @author prasad memane
 * @author swapnil mahajan
 */
public class S3FileObject implements Comparable<S3FileObject>{

	private String name;
	private Long size;
	
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Long getSize() {
		return size;
	}
	public void setSize(Long size) {
		this.size = size;
	}
	
	public S3FileObject(String name, Long size) {
		this.name = name;
		this.size = size;
	}
	@Override
	public int compareTo(S3FileObject o) {
		return this.size.compareTo(o.getSize());
	}
		
}
