package neu.mr.cs6240.utils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.ec2.model.StopInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;

/**
 * used to parse the credentials file which is needed by the AWS SDK to
 * bootstrap the master and slaves
 * 
 * @author ajay subramanya
 *
 */
public class EC2 {
	private String image;
	private String instance;
	private int minCount;
	private int maxCount;
	private String keyName;
	private String secGroup;
	private boolean monitoring;
	private String iamRole;
	private String region;
	private Map<String, String> regions;
	AmazonEC2Client ec2Client = new AmazonEC2Client();

	/**
	 * default constructor
	 * 
	 * @param file
	 */
	public EC2(String file) {
		populateRegions();
		Map<String, String> hm = getParams(file);
		this.image = hm.get("image");
		this.instance = hm.get("instance");
		this.keyName = hm.get("keyName");
		this.secGroup = hm.get("secGroup");
		this.monitoring = Boolean.parseBoolean(hm.get("monitoring"));
		this.iamRole = hm.get("iamRole");
		this.region = this.regions.get(hm.get("region"));
		ec2Client.setEndpoint(this.getRegion());
	}

	/**
	 * loops over each line in the file and gets the params and their values
	 * 
	 * @param file
	 * @return
	 */
	private Map<String, String> getParams(String file) {
		Map<String, String> hm = new HashMap<>();
		String[] lines = null;
		try {
			lines = FileUtils.readFileToString(new File(file)).split("\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
		for (String line : lines) {
			String[] attrs = line.split("=");
			hm.put(attrs[0].trim(), attrs[1].trim());
		}
		return hm;
	}

	/**
	 * 
	 * @param s
	 *            the string
	 * @return integer value of the String
	 */
	public int I(String s) {
		return Integer.parseInt(s);
	}

	/**
	 * populates the regions in a hashmap. Will not scale to new regions
	 */
	private void populateRegions() {
		this.regions = new HashMap<>();
		this.regions.put("us-east-1", "ec2.us-east-1.amazonaws.com");
		this.regions.put("us-west-1", "ec2.us-west-1.amazonaws.com");
		this.regions.put("us-west-2", "ec2.us-west-2.amazonaws.com");

	}

	/**
	 * 
	 * @param script
	 *            the boot-up script that would be sent as UserData
	 * @return the request object
	 * @throws IOException
	 */
	public RunInstancesRequest bootstrap(String script) throws IOException {
		RunInstancesRequest runInstancesRequest = new RunInstancesRequest();

		runInstancesRequest.withImageId(getImage()).withInstanceType(getInstance()).withMinCount(getMinCount())
				.withMaxCount(getMaxCount()).withKeyName(getKeyName()).withSecurityGroupIds(getSecGroup())
				.withMonitoring(isMonitoring()).withUserData(script)
				.withIamInstanceProfile(new IamInstanceProfileSpecification().withName(getIamRole()));

		return runInstancesRequest;
	}

	/**
	 * 
	 * @param instanceId
	 */
	public void stopEC2(String instanceId) {
		StopInstancesRequest stopInstancesRequest = new StopInstancesRequest(
				Arrays.asList(new String[] { instanceId }));
		StopInstancesResult stopInstancesResult = ec2Client.stopInstances(stopInstancesRequest);
		System.out.println("stop result : " + stopInstancesResult.toString());
	}

	/**
	 * 
	 * @param instanceId
	 */
	public void terminateEC2(String instanceId) {
		TerminateInstancesRequest terminate = new TerminateInstancesRequest(Arrays.asList(new String[] { instanceId }));
		TerminateInstancesResult terminateRes = ec2Client.terminateInstances(terminate);
		System.out.println("terminate result : " + terminateRes.toString());
	}

	@Override
	public String toString() {
		return "EC2 [image=" + image + ", instance=" + instance + ", minCount=" + minCount + ", maxCount=" + maxCount
				+ ", keyName=" + keyName + ", secGroup=" + secGroup + ", monitoring=" + monitoring + ", iamRole="
				+ iamRole + "]";
	}

	public String getImage() {
		return image;
	}

	public void setImage(String image) {
		this.image = image;
	}

	public String getInstance() {
		return instance;
	}

	public void setInstance(String instance) {
		this.instance = instance;
	}

	public int getMinCount() {
		return minCount;
	}

	public void setMinCount(int minCount) {
		this.minCount = minCount;
	}

	public int getMaxCount() {
		return maxCount;
	}

	public void setMaxCount(int maxCount) {
		this.maxCount = maxCount;
	}

	public String getKeyName() {
		return keyName;
	}

	public void setKeyName(String keyName) {
		this.keyName = keyName;
	}

	public String getSecGroup() {
		return secGroup;
	}

	public void setSecGroup(String secGroup) {
		this.secGroup = secGroup;
	}

	public boolean isMonitoring() {
		return monitoring;
	}

	public void setMonitoring(boolean monitoring) {
		this.monitoring = monitoring;
	}

	public String getIamRole() {
		return iamRole;
	}

	public void setIamRole(String iamRole) {
		this.iamRole = iamRole;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

}
