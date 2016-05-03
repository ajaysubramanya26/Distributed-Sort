package neu.mr.cs6240.netty_client;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import neu.mr.cs6240.TaskExceutor.OutputComparator;
import neu.mr.cs6240.TaskExceutor.OutputData;
import neu.mr.cs6240.aws.s3.S3Operations;

/**
 * Slave server's handler. It receives partition data from other slaves and
 * stores in memory. Once data for all slaves is received it along with its
 * local partition will merge the results and write the final sorted results to
 * s3
 * 
 * @author prasad
 * @author ajay
 *
 */
public class SlaveServerHandler extends SimpleChannelInboundHandler<Object> {

	final Logger logger = Logger.getLogger(SlaveServerHandler.class);

	static final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
	static int numOfNodes;
	static int receivedCount = 0;
	private String bucketName;
	static List<OutputData> finalSortList2;
	private Channel serverChannel;

	public SlaveServerHandler(int numOfNodes, String bucketName) throws FileNotFoundException {
		this.numOfNodes = numOfNodes;
		this.bucketName = bucketName;
		if (finalSortList2 == null) finalSortList2 = Collections.synchronizedList(new ArrayList<OutputData>());
	}

	public SlaveServerHandler(int numOfNodes, String bucketName, Channel serverChannel) throws FileNotFoundException {
		this.numOfNodes = numOfNodes;
		this.serverChannel = serverChannel;
		this.bucketName = bucketName;
		if (finalSortList2 == null) finalSortList2 = Collections.synchronizedList(new ArrayList<OutputData>());
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		System.out.println("In Slave Server Handler: Channel active");
		channels.add(ctx.channel());
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {

		OutputData od = (OutputData) msg;
		//Check if it is the last object sent by other clients and increment a counter accordingly
		if (!(od.getWban() == -10000)) {
			if (od != null) finalSortList2.add(od);
		} else {
			receivedCount++;
			logger.info("Recieved from " + receivedCount + " slaves");
		}

		// check if all slaves sent the required partitions
		if (receivedCount == numOfNodes - 1) {
			while (ClientHandler.flag != 1) {
				this.wait(2000);
			}

			//Not Required: However just in case the objects gets null over the network
			finalSortList2.removeAll(Collections.singleton(null));

			finalSortList2.addAll(ClientHandler.finalSortList1);
			Collections.sort(finalSortList2, new OutputComparator());

			try (BufferedWriter bw = new BufferedWriter(
					new FileWriter(new File("part-r-" + ClientHandler.finalOutputFileName)))) {				
				for (OutputData res : finalSortList2) {
					bw.write(res.getWban() + "," + res.getDate() + "," + res.getTime() + "," + res.getDryBulbTemp() + "\n");
				}				
			} catch (IOException e) {
				logger.error("Error while writing the final output: " + e);
			}

			// write finalSorted Output to s3 using the
			// finalOutputFileName(part-0000[x] format)
			// also inform the master about completion status using fin# msg
			logger.info("Done Writing final Output!!!");
			S3Operations s3 = new S3Operations();
			String bucket = ClientHandler.bucketName;
			s3.uploadFile(bucket, "OutputA9/" + "part-r-" + ClientHandler.finalOutputFileName,
					new File("part-r-" + ClientHandler.finalOutputFileName));
			logger.info("final output written to S3 @ " + bucket + "/OutputA9");
			this.serverChannel.writeAndFlush("fin#" + 0);
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

}