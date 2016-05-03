package neu.mr.cs6240.TaskExceutor;

import static neu.mr.cs6240.TaskExceutor.Configurations.NUM_OF_THREADS_SPAWN_MAP_TASK;
import static neu.mr.cs6240.TaskExceutor.Configurations.TIME_OUT_SECS_MAP_TASK_TO_FAIL;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import neu.mr.cs6240.netty_client.Msgs;
import neu.mr.cs6240.utils.CopyFilesS3ToLocal;
import neu.mr.cs6240.utils.FileUtils;

/**
 * Exceutes map task(i.e. reads each file given by master for this slave and
 * stores the data locally and finally sorts the data for all the files and
 * writes the output to file and returns pivots to master)
 *
 * @author smitha
 * @author prasad
 */
public class SortExceutor {

	private int numOfSlaves;
	final static Logger logger = Logger.getLogger(SortExceutor.class);

	public SortExceutor(int numOfSlaves) {
		this.numOfSlaves = numOfSlaves;
	}

	/**
	 * Read the metadata received from master and execute necessary
	 * prerequisites
	 *
	 * @param config
	 * @return
	 */
	public int init(String config) {
		FileUtils.deleteFolder(new File(FileUtils.DIR_S3_FILES_ON_LOCAL));
		FileUtils.createDirectory(FileUtils.DIR_S3_FILES_ON_LOCAL);

		File opfile = new File(FileUtils.FINAL_LOCAL_OUTPUT);
		if (opfile.exists()) {
			opfile.delete();
		}
		CopyFilesS3ToLocal objS3Local = new CopyFilesS3ToLocal();
		return objS3Local.readS3FilesToLocal(config, FileUtils.DIR_S3_FILES_ON_LOCAL);

	}

	/**
	 *
	 * @param lstSortRes
	 * @return
	 */
	public Object excuteSortJob(List<OutputData> lstSortRes) {
		// create a thread pool of 5 threads
		ExecutorService exceutor = Executors.newFixedThreadPool(NUM_OF_THREADS_SPAWN_MAP_TASK);

		// read the files downloaded
		File folder = new File(FileUtils.DIR_S3_FILES_ON_LOCAL);
		File[] listOfFiles = folder.listFiles();
		int num = 0;
		for (File file : listOfFiles) {
			if (file.isFile()) {
				num++;
				Future<List<OutputData>> fut = exceutor.submit(new FileSorter(file.getAbsolutePath(), num));
				try {
					List<OutputData> temp = fut.get(TIME_OUT_SECS_MAP_TASK_TO_FAIL, TimeUnit.SECONDS);

					lstSortRes.addAll(temp);
				} catch (TimeoutException e) {
					logger.warn("Timeout : Map task on file could not processed" + file.getAbsolutePath());
					fut.cancel(true);
				} catch (InterruptedException | ExecutionException e) {
					logger.error("Interruption error or Execution error in executeSortJob", e);
					return getSortError();
				}
			}
		}
		exceutor.shutdown();
		try {
			exceutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			logger.error("Interruption error", e);
			return getSortError();
		}
		return mergeSortRes(lstSortRes);
	}

	/**
	 * Sort the Results from all the files read from this machine
	 *
	 * @param lstSortRes
	 * @return
	 */
	private Object mergeSortRes(List<OutputData> lstSortRes) {
		System.out.println("Reducer Final Sort : Start");

		try {
			Collections.sort(lstSortRes, new OutputComparator());
		} catch (OutOfMemoryError o) {
			logger.error("error no space for final localSort", o);
			return getSortError();
		}

		try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(FileUtils.FINAL_LOCAL_OUTPUT)))) {
			for (OutputData res : lstSortRes) {
				bw.write(res.getWban() + "," + res.getDate() + "," + res.getTime() + "," + res.getDryBulbTemp() + "\n");
			}
		} catch (IOException e) {
			logger.error("error in writing final localSort output", e);
			return getSortError();
		}
		logger.info("Reducer Final Sort : Finish");

		return extractMetaData(lstSortRes);

	}

	/**
	 * Error msg to server when sort fails
	 * 
	 * @return
	 */
	private String getSortError() {
		return Msgs.RETURN_MSG_SORTRES + "-1";
	}

	/**
	 * Gets the number of bytes for a serializable object
	 *
	 * @param obj
	 * @return
	 * @throws IOException
	 */
	public static int sizeOf(Object obj) {

		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		try {
			ObjectOutputStream objStream = new ObjectOutputStream(byteStream);

			objStream.writeObject(obj);
			objStream.flush();
			objStream.close();
		} catch (IOException e) {
			logger.error("Spill operation disrupted", e);
		}

		return byteStream.toByteArray().length;
	}

	/**
	 * Get the pivots for the locally sorted files. Based on the number of slave
	 * nodes.
	 *
	 * @param finalReduceRes
	 * @return
	 */
	private Object extractMetaData(List<OutputData> finalReduceRes) {
		int len = finalReduceRes.size();

		String returnPivots = "@";

		for (int i = 0; i < numOfSlaves; i++) {
			if (returnPivots.equals("@"))
				returnPivots += finalReduceRes.get(i * len / numOfSlaves).getDryBulbTemp().toString();
			else
				returnPivots += "," + finalReduceRes.get(i * len / numOfSlaves).getDryBulbTemp().toString();
		}

		return Msgs.RETURN_MSG_SORTRES + "0," + returnPivots;
	}
}
