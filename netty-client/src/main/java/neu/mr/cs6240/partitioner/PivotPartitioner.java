package neu.mr.cs6240.partitioner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import neu.mr.cs6240.TaskExceutor.OutputData;

/**
 * Pivoting logic
 * 
 * @author prasad
 * @author swapnil
 *
 */
public class PivotPartitioner {

	private String pivots;
	private List<Double> pivotsList = new ArrayList<Double>();

	public PivotPartitioner(String pivots) {
		this.pivots = pivots;

		String[] pivotsArray = pivots.split(",");
		for (String pivot : pivotsArray) {
			pivotsList.add(Double.parseDouble(pivot));
		}
	}

	public Object getParitionCount(List<OutputData> lstSortRes, Map<String, Pair<Integer, Integer>> partitions) {
		int pivotIndex = 0;
		String response = "partitions#";
		// OutputData prev = null;
		Integer prevPivotIndex = Integer.MAX_VALUE;

		for (OutputData res : lstSortRes) {

			if (res.getDryBulbTemp() < pivotsList.get(pivotIndex)) {
				continue;
				// prev = res;
			}

			else {
				if (pivotIndex == 0) {
					prevPivotIndex = lstSortRes.indexOf(res) - 1;

					String bucket = "0-" + pivotsList.get(pivotIndex);
					int count = lstSortRes.indexOf(res);

					partitions.put(bucket, Pair.of(0, prevPivotIndex));
					response += bucket + ":" + count;

					writePartitionToFile(bucket, 0, prevPivotIndex, lstSortRes);
					if (pivotIndex == pivotsList.size() - 1) {
						response = lastParition(response, lstSortRes, pivotIndex, prevPivotIndex, res, partitions);
						break;
					}

					pivotIndex++;
				} else {
					String bucket = pivotsList.get(pivotIndex - 1) + "-" + pivotsList.get(pivotIndex);
					int count = lstSortRes.indexOf(res) - 1 - prevPivotIndex;

					partitions.put(bucket, Pair.of(prevPivotIndex, lstSortRes.indexOf(res) - 1));
					// prevPivotIndex = lstSortRes.indexOf(res) - 1;

					response += "," + bucket + ":" + count;

					// System.out.println("1st: " + response);
					writePartitionToFile(bucket, prevPivotIndex, lstSortRes.indexOf(res) - 1, lstSortRes);

					prevPivotIndex = lstSortRes.indexOf(res) - 1;

					if (pivotIndex == pivotsList.size() - 1) {
						response = lastParition(response, lstSortRes, pivotIndex, prevPivotIndex, res, partitions);
						break;
					}

					pivotIndex++;
				}
			}
		}

		return response;
	}

	public void writePartitionToFile(String bucket, int start, int end, List<OutputData> lstSortRes) {
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(bucket)))) {
			for (OutputData res : lstSortRes.subList(start, end)) {
				bw.write(res.getWban() + "," + res.getDate() + "," + res.getTime() + "," + res.getDryBulbTemp() + "\n");
			}
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String lastParition(String response, List<OutputData> lstSortRes, int pivotIndex, Integer prevPivotIndex,
			OutputData res, Map<String, Pair<Integer, Integer>> partitions) {

		String bucket = pivotsList.get(pivotIndex) + "-N";
		int count = lstSortRes.size() - 1 - prevPivotIndex;

		partitions.put(bucket, Pair.of(prevPivotIndex, lstSortRes.size() - 1));

		response += "," + bucket + ":" + count;

		// System.out.println("last: " + response);
		writePartitionToFile(bucket, prevPivotIndex, lstSortRes.size() - 1, lstSortRes);
		return response;
	}

	public void setPivotsList(List<Double> pivotsList) {
		this.pivotsList = pivotsList;
	}

	public String getPivots() {
		return pivots;
	}

	public void setPivots(String pivots) {
		this.pivots = pivots;
	}

	public List<Double> getPivotsList() {
		return pivotsList;
	}

}
