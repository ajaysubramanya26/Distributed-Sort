package neu.mr.cs6240.netty_client;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import neu.mr.cs6240.TaskExceutor.ERR_CODE;
import neu.mr.cs6240.TaskExceutor.OutputData;
import neu.mr.cs6240.TaskExceutor.SortExceutor;
import neu.mr.cs6240.aws.s3.ReadConfig;
import neu.mr.cs6240.partitioner.PivotPartitioner;

public class ClientHandler extends SimpleChannelInboundHandler<String> {
	final Logger logger = Logger.getLogger(ClientHandler.class);
	static List<OutputData> lstSortRes = new ArrayList<OutputData>();
	static Map<String, Pair<Integer, Integer>> partitions = new HashMap<String, Pair<Integer, Integer>>();
	private static int numOfSlaves;
	static List<OutputData> finalSortList;
	private ReadConfig s3config;
	static List<OutputData> finalSortList1;
	static int flag = 0;
	static String finalOutputFileName;
	public static String bucketName;
	private static Channel masterChannel;

	public ClientHandler(int numOfSlaves) {
		this.numOfSlaves = numOfSlaves;
		s3config = new ReadConfig();
	}

	/**
	 * @param ctx
	 *            the context to which you write and if there are no more
	 *            handlers in the pipeline then what ever you write to context
	 *            will be written to the channel
	 */
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		logger.info("Client writing to server");
		ctx.writeAndFlush(Msgs.RETURN_MSG_READY + "0");
	}

	/**
	 * @param ctx
	 *            the context to which you write and if there are no more
	 *            handlers in the pipeline then what ever you write to context
	 *            will be written to the channel
	 * @param msg
	 *            the message that is sent from the remote of the channel
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {

		String localAddress = getPublicIp();
		String[] command = msg.split("#");

		if (command.length < 1)
			logger.info(INFO(ctx) + "command not found " + msg);

		else {
			String config = msg.replaceFirst(Msgs.SORT_CMD + "#", "");
			switch (command[0]) {

			case Msgs.SORT_CMD:
				logger.info("Received sort msg from server: " + command[1]);
				s3config.readS3ConfigurationFile(config);
				ctx.writeAndFlush(sortFilesLocallyHandler(config, lstSortRes));
				break;

			case "pivots":
				logger.info("Pivots are: " + command[1]);
				ctx.writeAndFlush(getPartitions(command[1]));
				lstSortRes.clear();
				break;

			case Msgs.SEND_TO:
				logger.info("Received sendTo msg from server: " + command[1]);

				finalSortList1 = new ArrayList<>();

				String[] ipBuckets = command[1].split(",");

				for (String ipBucket : ipBuckets) {

					String[] ipb = ipBucket.split("@");
					String[] hostPort = ipb[0].split(":");
					String host = hostPort[0].replace("/", "").trim();
					String rcdBucket = ipb[1];
					String localHost = localAddress.split(":")[0].replace("/", "").trim();

					if (!host.equals(localHost)) {
						logger.info("Sending to host" + host);
						this.bucketName = s3config.getBUCKET_NAME();
						this.masterChannel = ctx.channel();
						EventLoopGroup worker = new NioEventLoopGroup();
						Bootstrap b = new Bootstrap().group(worker).channel(NioSocketChannel.class)
								.option(ChannelOption.SO_KEEPALIVE, true)
								.handler(new SlaveServerInitializer(numOfSlaves, bucketName, masterChannel))
								.option(ChannelOption.SO_SNDBUF, 128000000).option(ChannelOption.SO_RCVBUF, 128000000);
						Channel ch = b.connect(host, 8993).sync().channel();

						BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(rcdBucket)));

						String line = null;
						while ((line = br.readLine()) != null) {
							String[] s = line.split(",");
							ch.writeAndFlush(new OutputData(Integer.parseInt(s[0]), Integer.parseInt(s[1]),
									Integer.parseInt(s[2]), Double.parseDouble(s[3])));
						}

						OutputData od = new OutputData(-10000, 0, 0, 0.0);
						ch.write(od);
						ch.flush();

						br.close();
					} else {
						finalOutputFileName = rcdBucket;
						String line = null;
						try (BufferedReader br = new BufferedReader(
								new InputStreamReader(new FileInputStream(rcdBucket)))) {
							while ((line = br.readLine()) != null) {
								String[] s = line.split(",");
								finalSortList1.add(new OutputData(Integer.parseInt(s[0]), Integer.parseInt(s[1]),
										Integer.parseInt(s[2]), Double.parseDouble(s[3])));
							}
						}
						flag = 1;
					}
				}
				break;

			default:
				logger.error(INFO(ctx) + "command not found " + command[0]);
				ctx.writeAndFlush(Msgs.RETURN_MSG_FAILURE + ERR_CODE.INVALID_COMMAND_FROM_SERVER.val());
			}
		}
	}

	/**
	 * when we are done reading
	 */
	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		ctx.flush();
	}

	/**
	 * closing the channel when an exception occurs
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		cause.printStackTrace();
		ctx.close();
	}

	private String INFO(ChannelHandlerContext ctx) {
		return ctx.channel().localAddress().toString();
	}

	/**
	 *
	 * @param config
	 *            the configuration file that was received from the server
	 * @return the sorted metadata
	 */
	private static Object sortFilesLocallyHandler(String config, List<OutputData> lstSortRes) {
		SortExceutor sortExec = new SortExceutor(numOfSlaves);
		int res = sortExec.init(config);
		if (res == ERR_CODE.FILES_READ_SUCESSFULLY_FROM_S3.val()) {
			return sortExec.excuteSortJob(lstSortRes);
		} else {
			return Msgs.RETURN_MSG_SORTRES + "-1";
		}
	}

	private Object getPartitions(String pivots) {
		PivotPartitioner pp = new PivotPartitioner(pivots);
		return pp.getParitionCount(lstSortRes, partitions);
	}

	/**
	 *
	 * @return the public IP of the machine
	 */
	private String getPublicIp() {
		URL connection;
		String str = null;
		try {
			connection = new URL("http://checkip.amazonaws.com/");
			URLConnection con = connection.openConnection();
			BufferedReader reader = new BufferedReader(new InputStreamReader(con.getInputStream()));
			str = reader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return str;
	}
}
