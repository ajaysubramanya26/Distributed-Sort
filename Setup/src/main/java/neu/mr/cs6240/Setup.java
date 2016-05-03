package neu.mr.cs6240;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.amazonaws.AmazonClientException;

import neu.mr.cs6240.aws.s3.S3Operations;
import neu.mr.cs6240.master.Master;
import neu.mr.cs6240.slave.Slave;
import neu.mr.cs6240.utils.EC2;
import neu.mr.cs6240.utils.Utils;

/**
 * Initialises the master and slave
 * 
 * @author ajay subramanya
 */
public class Setup {
	private static String log4jConfPath = "./log4jsetup.properties";
	private final static Logger logger = Logger.getLogger(Setup.class);
	private static final int EC2_DETAILS = 0;
	private static final int MASTER_SCRIPT = 1;
	private static final int SLAVE_SCRIPT = 2;
	private static final int NUM_NODES = 3;
	private static final int S3_BUCKET = 4;
	private static final String SUCCESS = "OutputA9/_SUCCESS";
	private static final String FAILURE = "OutputA9/_FAILURE";

	/**
	 *
	 * @param args
	 *            ec2Details, masterScript ,clientScript, number of nodes, s3
	 *            bucket
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		List<String> instances = new ArrayList<>();
		PropertyConfigurator.configure(log4jConfPath);
		Master master = new Master(args[EC2_DETAILS], args[MASTER_SCRIPT], args[NUM_NODES]);
		master.start();
		instances.add(master.getInstance());
		Slave slave = new Slave(args[EC2_DETAILS], args[SLAVE_SCRIPT], args[NUM_NODES], master.getIp());
		slave.start();
		instances.addAll(slave.getInstances());
		pollS3(args[EC2_DETAILS], args[S3_BUCKET], instances);
	}

	/**
	 * 
	 * @param ec2Details
	 *            the ec2 details that are needed to use EC2
	 * @param bucketName
	 *            the name of the bucket
	 * @param instances
	 *            the instances that need to be polled and stopped
	 */
	private static void pollS3(String ec2Details, String bucketName, List<String> instances) {
		S3Operations s3 = new S3Operations();
		EC2 ec2 = new EC2(ec2Details);
		logger.info("polling for completion ...");
		while (true) {
			try {
				List<String> files = new ArrayList<>();
				files.addAll(s3.listObjsInBucket(bucketName, "OutputA9"));

				if (files.contains(SUCCESS) || files.contains(FAILURE)) {
					for (String i : instances) {
						logger.info("stopping instance " + i
								+ " . please terminate the instance after looking at the logs");
						ec2.stopEC2(i);
					}
					break;
				}
				Utils.sleep();
			} catch (AmazonClientException e) {
				Utils.logClientError(e);
				logger.error("unable to terminate instances , please do it manually");
				break;
			}
		}

	}
}
