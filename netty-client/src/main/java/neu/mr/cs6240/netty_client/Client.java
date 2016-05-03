package neu.mr.cs6240.netty_client;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Slave Main program using netty. This program will connect to master as well
 * will start its own server so other clients can send data during shuffle phase.
 *
 *
 * @author ajay subramanya
 * @author smitha
 * @author prasad
 * @author swapnil
 */

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class Client {

	// Master listening port for slaves
	static final int MASTER_PORT = Integer.parseInt(System.getProperty("port", "8992"));
	static final int CHANNEL_BUF_SIZE = 128000000;
	static final int SLAVE_SERVER_PORT = 8993;

	public static void main(String[] args) throws Exception {

		String log4jConfPath = "./log4j.properties";
		PropertyConfigurator.configure(log4jConfPath);
		final Logger logger = Logger.getLogger(Client.class);

		String masterIp = "127.0.0.1"; // Default
		int numOfSlaves = 1; // Default

		if (args.length != 2) {
			logger.error("Master's IP and Number of nodes not specified Format: <IP NumberOfSlaves>");
			System.exit(-1);
		}

		masterIp = args[0];

		try {
			numOfSlaves = Integer.parseInt(args[1]);
		} catch (NumberFormatException e) {
			logger.error("Invalid Number given for Number of Slaves argument", e);
			System.exit(-1);
		}

		logger.info(
				"Connecting to master with ip " + masterIp + " on port " + MASTER_PORT + " numOfSlaves " + numOfSlaves);
		EventLoopGroup worker = new NioEventLoopGroup();

		try {
			logger.info("Bootstraping slave client");
			Bootstrap b = new Bootstrap().group(worker).channel(NioSocketChannel.class)
					.option(ChannelOption.SO_KEEPALIVE, true).handler(new ClientInitializer(numOfSlaves))
					.option(ChannelOption.SO_SNDBUF, CHANNEL_BUF_SIZE)
					.option(ChannelOption.SO_RCVBUF, CHANNEL_BUF_SIZE);
			Channel ch = b.connect(args[0], MASTER_PORT).sync().channel();

			EventLoopGroup master = new NioEventLoopGroup(1);
			EventLoopGroup sWorker = new NioEventLoopGroup();
			try {
				logger.info("Bootstraping slave server");
				ServerBootstrap sb = new ServerBootstrap().group(master, sWorker).channel(NioServerSocketChannel.class)
						.childHandler(new SlaveServerInitializer(numOfSlaves, ch))
						.childOption(ChannelOption.SO_KEEPALIVE, true).option(ChannelOption.SO_KEEPALIVE, true)
						.childOption(ChannelOption.SO_SNDBUF, CHANNEL_BUF_SIZE)
						.childOption(ChannelOption.SO_RCVBUF, CHANNEL_BUF_SIZE);

				sb.bind(SLAVE_SERVER_PORT).channel().closeFuture().sync();

			} finally {
				// sWorker.shutdownGracefully();
				// master.shutdownGracefully();
			}

		} finally {
			// worker.shutdownGracefully();
		}
	}
}
