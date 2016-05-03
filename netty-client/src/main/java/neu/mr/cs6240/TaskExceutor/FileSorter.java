package neu.mr.cs6240.TaskExceutor;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVParser;

/**
 * Reads each file and gives the List<OutputData>
 *
 * @author smitha
 */
public class FileSorter implements Callable<List<OutputData>> {

	final Logger logger = Logger.getLogger(FileSorter.class);
	private String inputFile;
	int num;

	public FileSorter(String fileName, int num) {
		inputFile = fileName;
		this.num = num;
	}

	/**
	 * An abstraction of map task Where each line from input data is read and
	 * given to mapper map function and data is return to context. Context here
	 * is List<OutputData>
	 */
	@Override
	public List<OutputData> call() throws Exception {
		logger.info("FileSorter " + num + " exceuting");
		List<OutputData> lstOutput = new ArrayList<OutputData>();

		String line = "";

		try (BufferedReader br = getFileReader()) {
			while ((line = br.readLine()) != null) {
				map(line, lstOutput);
			}

		} catch (IOException io) {
			logger.error("IO exception in call in FileSorter", io);

		}
		return lstOutput;
	}

	/**
	 * Map task which processes each line
	 *
	 * @param line
	 * @param lstOutput
	 */
	private void map(String line, List<OutputData> lstOutput) {
		CSVParser csvReader = new CSVParser(',', '"');

		try {
			String[] tokens = csvReader.parseLine(line);
			if (tokens.length >= 8 && isDataValid(tokens)) {
				try {
					lstOutput.add(new OutputData(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]),
							Integer.parseInt(tokens[2]), Double.parseDouble(tokens[8])));
				} catch (NumberFormatException nfe) {
					return;
				}
			}
		} catch (IOException e) {
			// System.err.println("error in parsing line:" + inputFile + line);
			// ignore : some error in parsing the line
		}
	}

	/**
	 * Basic sanity test on the line in interested fields Wban, Date, Time,
	 * Dry_bulb_temp
	 *
	 * @param line
	 * @return
	 */
	private boolean isDataValid(String[] line) {
		return line[0] != null && !line[0].isEmpty() && line[1] != null && !line[1].isEmpty() && line[2] != null
				&& !line[2].isEmpty() && line[8] != null && !line[8].isEmpty() && !line[8].equals("-");
	}

	/**
	 * An abstraction for reading different files(codecs) types and giving a
	 * BufferReader as common interface
	 *
	 * @return
	 */
	private BufferedReader getFileReader() {
		InputStream in = null;

		try {
			String extension = inputFile.substring(inputFile.lastIndexOf(".") + 1, inputFile.length());

			switch (extension) {
			case "gz":
				in = new GZIPInputStream(new FileInputStream(inputFile));
				break;
			default:
				in = new FileInputStream(inputFile);
			}

		} catch (java.util.zip.ZipException e) {
			logger.warn(inputFile + " Not in GZIP format");
		} catch (IOException e1) {
			logger.error("IoException in getFileReader", e1);
		}
		return new BufferedReader(new InputStreamReader(in));
	}

}
