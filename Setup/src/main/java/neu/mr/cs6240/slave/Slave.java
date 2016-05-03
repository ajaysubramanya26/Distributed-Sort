package neu.mr.cs6240.slave;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.elasticmapreduce.model.InstanceState;

import neu.mr.cs6240.utils.EC2;
import neu.mr.cs6240.utils.Utils;

/**
 * abstraction to run the slave instance
 * 
 * @author ajay subramanya
 */
public class Slave {

	private String clientScript;
	private EC2 ec2;
	private String masterIp;
	private static AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
	private static AmazonEC2Client amazonEC2Client = new AmazonEC2Client(credentials);
	private final static Logger logger = Logger.getLogger(Slave.class);
	private List<String> instances;

	/**
	 * default constructor
	 * 
	 * @param credentials
	 *            the file which contains all the ec2 details
	 * @param script
	 *            the bootup run script
	 */
	public Slave(String ec2Details, String script, String slaveCount, String masterIp) {
		this.ec2 = new EC2(ec2Details);
		this.ec2.setMinCount(1);
		this.ec2.setMaxCount(Integer.parseInt(slaveCount) - 1);
		this.clientScript = script;
		this.masterIp = masterIp;
		this.instances = new ArrayList<>();
	}

	/**
	 * starts the slaves, number and other params set using constructor
	 * 
	 * @throws IOException
	 */
	public void start() throws IOException {
		logger.info("Starting slaves");
		try {
			String scriptArgs = this.masterIp + " " + this.ec2.getMaxCount();
			RunInstancesRequest runInstancesRequest = ec2.bootstrap(Utils.getUserDataScript(clientScript, scriptArgs));

			amazonEC2Client.setEndpoint(ec2.getRegion());
			RunInstancesResult runInstancesResult = amazonEC2Client.runInstances(runInstancesRequest);

			List<Instance> instanceIds = runInstancesResult.getReservation().getInstances();
			List<DescribeInstancesRequest> descReq = new ArrayList<>();
			for (Instance i : instanceIds) {
				descReq.add(new DescribeInstancesRequest().withInstanceIds(i.getInstanceId()));
			}

			logger.info("polling to check slaves state");

			while (true) {
				List<Instance> resInstances = new ArrayList<>();
				for (DescribeInstancesRequest dir : descReq) {
					DescribeInstancesResult res = amazonEC2Client.describeInstances(dir);
					resInstances.addAll(res.getReservations().get(0).getInstances());
				}

				List<String> completed = new ArrayList<>();
				for (Instance i : resInstances) {
					logger.info("instance " + i.getPublicIpAddress() + " is in " + i.getState().getName());
					if (i.getState().getName().equalsIgnoreCase(InstanceState.RUNNING.name())) {
						completed.add(i.getInstanceId());
						logger.info("client with public ip " + i.getPublicIpAddress() + " is now running");
					}
				}

				if (completed.size() == this.ec2.getMaxCount()) {
					logger.info("all clients now bootstrapped and running");
					this.instances.addAll(completed);
					return;
				}

				Utils.sleep();

			}

		} catch (AmazonServiceException ase) {
			Utils.logServiceError(ase);
		} catch (AmazonClientException ace) {
			Utils.logClientError(ace);
		}
	}

	public List<String> getInstances() {
		return instances;
	}

	public void setInstances(List<String> instances) {
		this.instances = instances;
	}

}
