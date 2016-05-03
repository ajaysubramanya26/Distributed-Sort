package neu.mr.cs6240.utils;

import java.io.File;

import org.apache.log4j.Logger;

/**
 * Basic common file/directory operations
 * 
 * @author smitha
 *
 */
public class FileUtils {

	final static Logger logger = Logger.getLogger(FileUtils.class);

	/**
	 * Creates a directory if not present
	 *
	 * @param dirName
	 */
	public static void createDirectory(String dirName) {
		File file = new File(dirName);
		if (!file.exists()) {
			if (file.mkdir()) {
				logger.info("Directory is created:" + dirName);
			} else {
				logger.error("Failed to create directory!");
			}
		}
	}

	public static void deleteFolder(File folder) {
		File[] files = folder.listFiles();
		if (files != null) { // some JVMs return null for empty dirs
			for (File f : files) {
				if (f.isDirectory()) {
					deleteFolder(f);
				} else {
					f.delete();
				}
			}
		}
		folder.delete();
	}

	public static final String DIR_S3_FILES_ON_LOCAL = "Temp_Files";
	public static final String FINAL_LOCAL_OUTPUT = "Output";
	public static final String CONFIG_FILE_NAME = "job.config";

}
