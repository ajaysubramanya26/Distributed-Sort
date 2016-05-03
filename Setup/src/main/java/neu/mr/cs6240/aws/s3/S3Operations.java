package neu.mr.cs6240.aws.s3;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;

/**
 *
 * @author ajay subramanya, smitha naresh
 * @info Wrapper for basic s3 operations
 */
public class S3Operations {

	private AmazonS3Client s3;
	private Logger logger;

	public S3Operations() {
		s3 = new AmazonS3Client();
		logger = Logger.getLogger(S3Operations.class);
	}

	private static final String SUFFIX = "/";
	private static final int BUFFER_SIZE = 128 * 1024 * 1024; // 128MB

	/**
	 * Create a bucket in specific region
	 *
	 * @param region
	 * @param bucketName
	 */
	public void createBucket(Region region, String bucketName) {
		try {
			s3.setRegion(region);
			s3.createBucket(bucketName);
		} catch (AmazonServiceException ase) {
			printServiceException(ase);
		} catch (AmazonClientException ace) {
			printClientException(ace);
		}
	}

	/**
	 * @author https://javatutorial.net/java-s3-example
	 * @param bucketName
	 * @param folderName
	 */
	public void createFolder(String bucketName, String folderName) {
		try {
			// create meta-data for your folder and set content-length to 0
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(0);
			// create empty content
			InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
			// create a PutObjectRequest passing the folder name suffixed by /
			PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, folderName + SUFFIX, emptyContent,
					metadata);
			// send request to S3Operations to create folder
			s3.putObject(putObjectRequest);
		} catch (AmazonServiceException ase) {
			printServiceException(ase);
		} catch (AmazonClientException ace) {
			printClientException(ace);
		}
	}

	public void listBuckets() {
		for (Bucket bucket : s3.listBuckets()) {
			System.out.println(bucket.getName());
		}
	}

	/**
	 * 
	 * @param bucketName
	 *            the name of the bucket
	 * @param key
	 *            the directory
	 * @param aFile
	 *            the file to upload
	 */
	public void uploadFile(String bucketName, String key, File aFile) {
		try {
			s3.putObject(new PutObjectRequest(bucketName, key, aFile));
		} catch (AmazonServiceException ase) {
			printServiceException(ase);
		} catch (AmazonClientException ace) {
			printClientException(ace);
		}
	}

	/**
	 * Uploads a directory from local to s3
	 *
	 * @param bucketName
	 * @param dirNameOnS3
	 * @param inputFilePath
	 *
	 *            Or you can block the current thread and wait for your transfer
	 *            to complete. If the transfer fails, this method will throw an
	 *            AmazonClientException or AmazonServiceException detailing the
	 *            reason.
	 */
	public void uploadDirectory(String bucketName, String dirNameOnS3, File inputFilePath) {
		TransferManager tx = null;
		try {
			tx = new TransferManager();
			MultipleFileUpload myUpload = tx.uploadDirectory(bucketName, dirNameOnS3, inputFilePath, true);
			myUpload.waitForCompletion();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			// After the upload is complete, call shutdownNow to release the
			// resources.
			if (tx != null) {
				tx.shutdownNow();
			}
		}

	}

	/**
	 * Downloads a directory from s3 to local destination path
	 *
	 * @param bucketName
	 * @param dirNameOnS3
	 * @param destlocalDirPath
	 */
	public void downloadDirectory(String bucketName, String dirNameOnS3, File destlocalDirPath) {
		TransferManager tx = new TransferManager();
		MultipleFileDownload myDownload = tx.downloadDirectory(bucketName, dirNameOnS3, destlocalDirPath);

		// Or you can block the current thread and wait for your transfer to
		// to complete. If the transfer fails, this method will throw an
		// AmazonClientException or AmazonServiceException detailing the reason.
		try {
			myDownload.waitForCompletion();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// After the upload is complete, call shutdownNow to release the
		// resources.
		tx.shutdownNow();
	}

	/**
	 * Prints all objects in given bucketName and prefix path
	 *
	 * @param bucketName
	 *            the name of the bucket
	 * @param prefix
	 *            the path of the files
	 * @return list of files in bucket
	 */
	public List<String> listObjsInBucket(String bucketName, String prefix)
			throws AmazonServiceException, AmazonClientException {
		List<String> files = new ArrayList<>();
		ObjectListing objectListing = s3
				.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(prefix));
		for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
			files.add(objectSummary.getKey());
		}
		return files;
	}

	/**
	 * Prints the contents of the file
	 *
	 * @param bucketName
	 * @param key
	 */
	public void downloadAndReadPrintFile(String bucketName, String key) {
		try {
			// Download and read an object (text file).
			S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
			logger.info(object.getObjectMetadata().getContentType());
			logger.info(object.getObjectMetadata().getContentLength());
			BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
			String line;

			while ((line = reader.readLine()) != null) {
				logger.info(line);
			}
		} catch (AmazonServiceException ase) {
			printServiceException(ase);
		} catch (AmazonClientException ace) {
			printClientException(ace);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Delete a file from bucket name
	 *
	 * @param bucketName
	 *            - s3 Bucket name
	 * @param key
	 *            - fileName
	 */
	public void deleteFile(String bucketName, String key) {
		try {
			s3.deleteObject(bucketName, key);
		} catch (AmazonServiceException ase) {
			printServiceException(ase);
		} catch (AmazonClientException ace) {
			printClientException(ace);
		}
	}

	/**
	 * Delete a bucket
	 *
	 * @param bucketName
	 */
	public void deleteBucket(String bucketName) {
		try {
			s3.deleteBucket(bucketName);
		} catch (AmazonServiceException ase) {
			printServiceException(ase);
		} catch (AmazonClientException ace) {
			printClientException(ace);
		}
	}

	/**
	 * Calculating size of file
	 *
	 * @param bucketName
	 * @param key
	 * @throws IOException
	 */
	public void readS3ObjectUsingByteArray(String bucketName, String key) {
		try {
			S3Object s3object = s3.getObject(new GetObjectRequest(bucketName, key));

			InputStream stream = s3object.getObjectContent();
			byte[] content = new byte[BUFFER_SIZE];

			int totalSize = 0;

			int bytesRead;
			while ((bytesRead = stream.read(content)) != -1) {
				totalSize += bytesRead;
			}
			logger.info("Total Size of file in MB = " + (double) totalSize / (1024 * 1024));
		} catch (AmazonServiceException ase) {
			printServiceException(ase);
		} catch (AmazonClientException ace) {
			printClientException(ace);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Copying the S3 File locally
	 *
	 * @param bucketName
	 * @param key
	 * @param outputFileName
	 * @throws IOException
	 */
	public void readS3CopyLocal(String bucketName, String key, String outputFileName) {

		try {
			S3Object s3object = s3.getObject(new GetObjectRequest(bucketName, key));

			InputStream stream = s3object.getObjectContent();
			byte[] content = new byte[BUFFER_SIZE];

			BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(outputFileName));
			int totalSize = 0;
			int bytesRead;
			while ((bytesRead = stream.read(content)) != -1) {
				outputStream.write(content, 0, bytesRead);
				totalSize += bytesRead;
			}
			logger.info("Total Size of file in bytes = " + totalSize);
			// close resource even during exception
			outputStream.close();
		} catch (AmazonServiceException ase) {
			printServiceException(ase);
		} catch (AmazonClientException ace) {
			printClientException(ace);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Prints AmazonClientException details
	 *
	 * @param ace
	 */
	private void printClientException(AmazonClientException ace) {
		logger.error("Caught an AmazonClientException, which means the client encountered "
				+ "a serious internal problem while trying to communicate with , "
				+ "such as not being able to access the network.");
		logger.error("Error Message: " + ace.getMessage());
	}

	/**
	 * Prints AmazonServiceException details
	 *
	 * @param ase
	 */
	private void printServiceException(AmazonServiceException ase) {
		logger.error("Caught an AmazonServiceException, which means your request made it "
				+ "to Amazon , but was rejected with an error response for some reason.");
		logger.error("Error Message:    " + ase.getMessage());
		logger.error("HTTP Status Code: " + ase.getStatusCode());
		logger.error("AWS Error Code:   " + ase.getErrorCode());
		logger.error("Error Type:       " + ase.getErrorType());
		logger.error("Request ID:       " + ase.getRequestId());
	}
}
