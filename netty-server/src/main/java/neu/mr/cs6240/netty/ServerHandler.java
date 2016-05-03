package neu.mr.cs6240.netty;

import java.io.BufferedWriter;

/**
 * @author ajay subramanya
 */

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import neu.mr.cs6240.aws.s3.ReadConfig;
import neu.mr.cs6240.aws.s3.S3FileObject;
import neu.mr.cs6240.aws.s3.S3Operations;
import neu.mr.cs6240.utils.Bucket;
import neu.mr.cs6240.utils.ClientState;
import neu.mr.cs6240.utils.Msgs;
import neu.mr.cs6240.utils.TransferFillesTracker;

/**
 * This is the handler class for the Server of String type
 * @author prasad memane
 * @authod swapnil mahajan
 * @author smitha
 * @author ajay
 *
 */
public class ServerHandler extends SimpleChannelInboundHandler<String> {

	final Logger logger = Logger.getLogger(Server.class);

	static final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
	private int numOfSlaves;
	static List<List<String>> filesList;
	static Map<String, Integer> nodeFiles = new HashMap<String, Integer>();
	static Integer nodeCounter = 0;
	static Map<String, ClientState> clientState = new HashMap<String, ClientState>();
	static List<Bucket> allBuckets = new ArrayList<>();
	static Map<String, Bucket> clientBucketMap = new HashMap<String, Bucket>();

	public ServerHandler(int numberOfSlaves) {
		this.numOfSlaves = numberOfSlaves;
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		logger.info("Master channel active");
		if (filesList == null) {
			filesList = new ArrayList<>();
			splitInput();
		}
		channels.add(ctx.channel());
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
		String[] command = msg.split("#");
		if (command.length < 1) {
			logger.info("Illegal command");
		} else {
			String clientAddress = ctx.channel().remoteAddress().toString();
			switch (command[0]) {
			
			case Msgs.READY_CMD:
				logger.info("Received ready msg" + " [client : " + ctx.channel().remoteAddress().toString() + " connected]");

				updateClientState(ctx.channel().remoteAddress().toString(), command);
				logger.info("Client State: " + clientState.toString());

				boolean allReady = checkAllInState(Msgs.READY_CMD);
				if (allReady) 
					getFilesToSend();

				break;
				
			case Msgs.SORTRES_CMD:
				if (command[1].equals("-1")) {
					logger.error("Client Unable to sort" + ctx.channel().remoteAddress() + "terminating!!!");
					logger.error("Terminating all instance due to sort failure");
					writeResult("_FAILURE");
					System.exit(-1);
				}

				logger.info("Sorting done at client " + ctx.channel().remoteAddress() + " Pivot metadata received " + command[1]);

				updateClientState(ctx.channel().remoteAddress().toString(), command);
				logger.info("Client State: " + clientState.toString());

				boolean sortResDone = checkAllInState(Msgs.SORTRES_CMD);
				logger.info("SortRes Recieved from all clients? " + sortResDone);
				if (sortResDone) 
					createPivots();
				
				break;

			case Msgs.PARTITION_CMD:
				updateClientState(clientAddress, command);
				logger.info("Client State: " + clientState.toString());

				boolean partitionReady = checkAllInState(Msgs.PARTITION_CMD);

				String[] allBucketCount = command[1].split(",");
				for (String bucketCount : allBucketCount) {
					String[] bucket = bucketCount.split(":");
					allBuckets.add(new Bucket(clientAddress, bucket[0], bucket[1]));
				}

				logger.info("Partitions recieved from all clients? " + partitionReady);
				if (partitionReady)
					mapClientToBuckets();
				
				break;

			case Msgs.FIN_CMD:
				updateClientState(clientAddress, command);
				logger.info("Client State: " + clientState.toString());
				
				if (checkAllInState(Msgs.FIN_CMD)) {
					logger.info("all clients have successfully written their output to S3");
					writeResult("_SUCCESS");
				}
				
				break;
				
			case "failure":
				if (command[1].equals("-100")) 
					logger.info("Invalid command sent to client");
				
				break;

			default:
				ctx.writeAndFlush("command not found" + command[0]);
				
			}
		}
	}

	private void writeResult(String res) {
		writeResFile(res);
		ReadConfig readConf = new ReadConfig();
		try {
			readConf.readS3ConfigurationFile(FileUtils.readFileToString(new File("job.config"), "utf-8"));
		} catch (IOException e) {
			logger.error("while reading the config file " + e.getMessage());
		}
		S3Operations s3 = new S3Operations();
		s3.uploadFile(readConf.getBUCKET_NAME(), "OutputA9/" + res, new File(res));

	}

	/**
	 * This method writes the file to S3
	 * @param res
	 */
	private void writeResFile(String res) {
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(res)))) {
			bw.write("");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This method distributes the files evenly across the slaves and sends the command to every client 
	 * specifying which files to download
	 * @throws IOException
	 */
	private void getFilesToSend() throws IOException {
		int nodeCount = 0;

		for (Channel ch : channels) {
			String ret = "";

			for (String file : filesList.get(nodeCount)) {
				if (ret.equals(""))
					ret = file;
				else
					ret = ret + "," + file;
			}
			logger.info("Sending files to client " + ch.remoteAddress() + " : " + ret);
			ch.writeAndFlush("sort#" + FileUtils.readFileToString(new File("job.config"), "utf-8") + ret + "\n");
			nodeCount++;
		}
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		ctx.flush();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		cause.printStackTrace();
		ctx.close();
	}

	/**
	 * This method updates the state of the client
	 * @param clientAddr
	 * @param command
	 */
	private void updateClientState(String clientAddr, String[] command) {
		clientState.put(clientAddr, new ClientState(command[0], command[1]));
	}

	/**
	 * This method checks if all the clients are in the same state
	 * @param state
	 * @return
	 */
	private boolean checkAllInState(String state) {
		if (clientState.size() != numOfSlaves) return false;

		for (Map.Entry<String, ClientState> entry : clientState.entrySet()) {
			if (!entry.getValue().getMsg().equals(state)) return false;
		}
		return true;
	}

	/**
	 * This method creates the pivots to send to the clients based on the pivots received from the clients 
	 */
	private void createPivots() {
		List<Double> clientPivots = new ArrayList<>();
		for (Map.Entry<String, ClientState> entry : clientState.entrySet()) {
			String[] pivots = entry.getValue().getResult().split("@")[1].split(",");
			for (String pivot : pivots)
				clientPivots.add(Double.parseDouble(pivot));
		}

		Collections.sort(clientPivots);

		String returnPivots = "";
		for (int i = 1; i < numOfSlaves; i++) {
			if (returnPivots.equals(""))
				returnPivots += clientPivots.get(i * numOfSlaves).toString();
			else
				returnPivots += "," + clientPivots.get(i * numOfSlaves).toString();
		}

		for (Channel c : channels)
			c.writeAndFlush("pivots#" + returnPivots);
	}

	/**
	 * This method decides which bucket would be assigned to which slave,
	 * This achieves data locality
	 */
	private void mapClientToBuckets() {
		Collections.sort(allBuckets, Collections.reverseOrder());
		for (Bucket b : allBuckets) {
			if (!clientBucketMap.containsKey(b.getClientAddress())) {
				boolean flag = false;
				for (Bucket b1 : clientBucketMap.values()) {
					if (b1.getRange().equals(b.getRange())) {
						flag = true;
						break;
					}
				}
				if (flag == false) {
					clientBucketMap.put(b.getClientAddress(), b);
				}
			}
		}

		String sendToData = "";
		for (Map.Entry<String, Bucket> entry : clientBucketMap.entrySet()) {
			if (sendToData.equals(""))
				sendToData += entry.getValue().getClientAddress() + "@" + entry.getValue().getRange();
			else
				sendToData += "," + entry.getValue().getClientAddress() + "@" + entry.getValue().getRange();
		}

		for (Channel c : channels)
			c.writeAndFlush("sendTo#" + sendToData);
	}

	/**
	 * This method splits the input files based on the total size into x number of sets
	 * where x is the number of slaves
	 * @throws IOException
	 */
	private void splitInput() throws IOException {
		List<TransferFillesTracker> tracker = new ArrayList<>();
		ReadConfig readConf = new ReadConfig();
		readConf.readS3ConfigurationFile(FileUtils.readFileToString(new File("job.config"), "utf-8"));
		S3Operations operation = new S3Operations(readConf.getACCESS_KEY(), readConf.getSECRET_KEY());
		List<S3FileObject> files = operation.listObjsInBucket(readConf.getBUCKET_NAME(),
				readConf.getDIR_FILES_TO_READ());

		for (int i = 0; i < numOfSlaves; i++) {
			tracker.add(new TransferFillesTracker(0L));
		}

		int nodeCnt = 0;
		while (files.size() != 0) {
			if (nodeCnt == numOfSlaves) {
				nodeCnt = 0;
				Collections.sort(tracker);
			}

			S3FileObject append = files.remove(0);
			TransferFillesTracker t = tracker.get(nodeCnt);
			t.getFiles().add(append.getName());
			t.setSize(t.getSize() + append.getSize());
			nodeCnt++;
		}

		for (int i = 0; i < numOfSlaves; i++) {
			logger.info("Assigned to Index " + i + " size : " + tracker.get(i).getSize());
			filesList.add(i, tracker.get(i).getFiles());
		}
	}

}
