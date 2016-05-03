package neu.mr.cs6240.utils;

import java.io.File;

import neu.mr.cs6240.TaskExceutor.ERR_CODE;
import neu.mr.cs6240.aws.s3.ReadConfig;
import neu.mr.cs6240.aws.s3.S3Operations;

/**
 * Copies the files in job.config to local directory
 *
 * @author smitha
 *
 */
public class CopyFilesS3ToLocal {

	public int readS3FilesToLocal(String config, String dirS3FilesOnLocal) {
		ReadConfig s3config = new ReadConfig();
		s3config.readS3ConfigurationFile(config);

		S3Operations s3Client = new S3Operations();

		for (String fileName : s3config.getFILES_TO_READ_SET()) {
			s3Client.readS3CopyLocal(s3config.getBUCKET_NAME(), s3config.getDIR_FILES_TO_READ() + "/" + fileName,
					dirS3FilesOnLocal + "/" + fileName);
		}

		return validateFilesDownloaded(dirS3FilesOnLocal, s3config);
	}

	private int validateFilesDownloaded(String dirS3FilesOnLocal, ReadConfig s3config) {
		File folder = new File(dirS3FilesOnLocal);
		File[] listOfFiles = folder.listFiles();
		int num = 0;
		for (File file : listOfFiles) {
			if (file.isFile()) {
				if (s3config.getFILES_TO_READ_SET().contains(file.getName())) num++;
			}
		}
		if (num != s3config.getFILES_TO_READ_SET().size()) {
			return ERR_CODE.FILES_NOT_DOWNLOADED_S3.val();
		}
		return ERR_CODE.FILES_READ_SUCESSFULLY_FROM_S3.val();
	}

}
