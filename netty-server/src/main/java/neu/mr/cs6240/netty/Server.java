package neu.mr.cs6240.netty;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * @author ajay subramanya
 */

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * This is the main Server class
 * @author ajay
 * @author smitha
 *
 */
public class Server {
	static final int PORT = Integer.parseInt(System.getProperty("port", "8992"));

	public static void main(String[] args) throws Exception {

		String log4jConfPath = "./log4j.properties";
		PropertyConfigurator.configure(log4jConfPath);
		final Logger logger = Logger.getLogger(Server.class);

		if (args.length != 1) {
			logger.error("Number of nodes not specified");
			System.exit(-1);
		}

		int numOfSlaves = 1; // Default
		try {
			numOfSlaves = Integer.parseInt(args[0]);
		} catch (NumberFormatException e) {
			logger.error("Invalid Number", e);
			System.exit(-1);
		}

		EventLoopGroup master = new NioEventLoopGroup(1);
		EventLoopGroup worker = new NioEventLoopGroup();
		try {
			logger.info("bootstraping server");
			ServerBootstrap b = new ServerBootstrap().group(master, worker).channel(NioServerSocketChannel.class)
					.childHandler(new ServerInitializer(numOfSlaves)).childOption(ChannelOption.SO_KEEPALIVE, true)
					.option(ChannelOption.SO_KEEPALIVE, true);

			b.bind(PORT).channel().closeFuture().sync();

		} finally {
			// worker.shutdownGracefully();
			// master.shutdownGracefully();
		}
	}
}
