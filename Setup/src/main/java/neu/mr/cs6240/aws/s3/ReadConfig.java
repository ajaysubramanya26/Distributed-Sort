package neu.mr.cs6240.aws.s3;

import java.util.HashSet;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Reads the job config file sent from the server
 * 
 * @author smitha
 *
 */
public class ReadConfig {

	public String getACCESS_KEY() {
		return ACCESS_KEY;
	}

	public void setACCESS_KEY(String aCCESS_KEY) {
		ACCESS_KEY = aCCESS_KEY;
	}

	public String getSECRET_KEY() {
		return SECRET_KEY;
	}

	public void setSECRET_KEY(String sECRET_KEY) {
		SECRET_KEY = sECRET_KEY;
	}

	public String getBUCKET_NAME() {
		return BUCKET_NAME;
	}

	public void setBUCKET_NAME(String bUCKET_NAME) {
		BUCKET_NAME = bUCKET_NAME;
	}

	public String getREGION() {
		return REGION;
	}

	public void setREGION(String rEGION) {
		REGION = rEGION;
	}

	private String ACCESS_KEY;
	private String SECRET_KEY;
	private String BUCKET_NAME;
	private String REGION;
	private HashSet<String> FILES_TO_READ_SET;
	private String DIR_FILES_TO_READ;

	public String getDIR_FILES_TO_READ() {
		return DIR_FILES_TO_READ;
	}

	public void setDIR_FILES_TO_READ(String dIR_FILES_TO_READ) {
		DIR_FILES_TO_READ = dIR_FILES_TO_READ;
	}

	public HashSet<String> getFILES_TO_READ_SET() {
		return FILES_TO_READ_SET;
	}

	public void setFILES_TO_READ_SET(String commaSepFiles) {

		FILES_TO_READ_SET = new HashSet<>();
		String[] files = commaSepFiles.split(",");
		if (files.length == 0) {
			logger.log(Level.ERROR, ">>s3.ReadConfig No files to read" + commaSepFiles);
		} else {
			for (String file : files) {
				FILES_TO_READ_SET.add(file);
			}
		}

	}

	private org.apache.log4j.Logger logger;

	public ReadConfig() {
		logger = Logger.getLogger(ReadConfig.class);
	}

	/**
	 * Read the s3 config file and set the fields
	 *
	 * @param config
	 */
	public void readS3ConfigurationFile(String config) {
		String[] lines = config.split("\n");
		for (String line : lines) {
			String[] tokens = line.trim().split("=");
			if (tokens.length != 2) {
				logger.info(">>s3.ReadConfig failed for line" + line);
				continue;
			}
			switch (tokens[0]) {
			case "ACCESS_KEY":
				setACCESS_KEY(tokens[1]);
				break;
			case "SECRET_KEY":
				setSECRET_KEY(tokens[1]);
				break;
			case "BUCKET_NAME":
				setBUCKET_NAME(tokens[1]);
				break;
			case "REGION":
				setREGION(tokens[1]);
				break;
			case "FILES_TO_READ":
				setFILES_TO_READ_SET(tokens[1]);
				break;
			case "DIR_FILES_TO_READ":
				setDIR_FILES_TO_READ(tokens[1]);
				break;
			default:
				logger.log(Level.ERROR, ">>s3.ReadConfig Invalid for line" + line);
			}
		}
	}
}
