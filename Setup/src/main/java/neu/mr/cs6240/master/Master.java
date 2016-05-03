package neu.mr.cs6240.master;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.elasticmapreduce.model.InstanceState;

import neu.mr.cs6240.utils.EC2;
import neu.mr.cs6240.utils.Utils;

/**
 * abstraction to run the master instance
 *
 * @author ajay subramanya
 */
public class Master {

	private EC2 ec2;
	private String serverScript;
	private String ip;
	private Integer clients;
	private final static Logger logger = Logger.getLogger(Master.class);
	private static AmazonEC2Client ec2Client = new AmazonEC2Client();
	private String instance;

	/**
	 * default constructor
	 *
	 * @param credentials
	 *            the file which contains all the ec2 details
	 * @param script
	 *            the boot-up run script
	 */
	public Master(String ec2Details, String script, String clients) {
		this.serverScript = script;
		this.clients = Integer.parseInt(clients) - 1;
		this.ec2 = new EC2(ec2Details);
		this.ec2.setMinCount(1);
		this.ec2.setMaxCount(1);
	}

	/**
	 * starts the master instance and polls until the machine is running,
	 * returns once the machine is running
	 *
	 * @throws IOException
	 */
	public void start() throws IOException {
		logger.info("starting master");
		try {
			ec2Client.setEndpoint(ec2.getRegion());
			RunInstancesRequest runInstancesRequest = ec2
					.bootstrap(Utils.getUserDataScript(this.serverScript, this.clients.toString()));
			RunInstancesResult runInstancesResult = ec2Client.runInstances(runInstancesRequest);

			this.instance = runInstancesResult.getReservation().getInstances().get(0).getInstanceId();
			DescribeInstancesRequest descReq = new DescribeInstancesRequest().withInstanceIds(this.instance);

			logger.info("master instance id : " + this.instance);
			logger.info("polling to check master state");
			while (true) {
				DescribeInstancesResult res = ec2Client.describeInstances(descReq);
				String state = res.getReservations().get(0).getInstances().get(0).getState().getName();
				if (state.equalsIgnoreCase(InstanceState.RUNNING.name())) {
					this.ip = res.getReservations().get(0).getInstances().get(0).getPublicIpAddress();
					logger.info("master started at IP : " + this.ip);
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

	public String getInstance() {
		return instance;
	}

	public void setInstance(String masterInstance) {
		this.instance = masterInstance;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}
}
