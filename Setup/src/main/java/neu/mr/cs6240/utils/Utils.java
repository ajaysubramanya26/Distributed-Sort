package neu.mr.cs6240.utils;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.util.Base64;

/**
 * static class to hold methods that could be used across the project
 * 
 * @author ajay subramanya
 *
 */
public class Utils {

	private final static Logger logger = Logger.getLogger(Utils.class);

	/**
	 * 
	 * @param script
	 *            the master script
	 * @return a base64 encoded string of unix script that will be executed
	 *         during master bootup
	 * @throws IOException
	 */
	public static String getUserDataScript(String script) throws IOException {
		String str = new String(FileUtils.readFileToString(new File(script)));
		return new String(Base64.encodeAsString(str.getBytes()));
	}

	/**
	 * 
	 * @param script
	 *            the slave script
	 * @param dynamic
	 *            any dynamic content you may want to append
	 * @return a base64 encoded string of unix script that will be executed
	 *         during booting of slave
	 * @throws IOException
	 */
	public static String getUserDataScript(String script, String dynamic) throws IOException {
		String str = new String(FileUtils.readFileToString(new File(script))).concat(" " + dynamic);
		return new String(Base64.encodeAsString(str.getBytes()));
	}

	/**
	 * puts thread to sleep
	 */
	public static void sleep() {
		try {
			Thread.sleep(10 * 1000);
		} catch (InterruptedException e) {
		}
	}

	/**
	 * logs any client exception
	 * 
	 * @param ace
	 */
	public static void logClientError(AmazonClientException ace) {
		logger.error("Caught an AmazonClientException, which means the client encountered "
				+ "a serious internal problem while trying to communicate with , "
				+ "such as not being able to access the network.");
		logger.error("Error Message: " + ace.getMessage());
	}

	/**
	 * logs any service exception
	 * 
	 * @param ase
	 */
	public static void logServiceError(AmazonServiceException ase) {
		logger.error("Caught an AmazonServiceException, which means your request made it "
				+ "to Amazon , but was rejected with an error response for some reason.");
		logger.error("Error Message:    " + ase.getMessage());
		logger.error("HTTP Status Code: " + ase.getStatusCode());
		logger.error("AWS Error Code:   " + ase.getErrorCode());
		logger.error("Error Type:       " + ase.getErrorType());
		logger.error("Request ID:       " + ase.getRequestId());
	}
}
