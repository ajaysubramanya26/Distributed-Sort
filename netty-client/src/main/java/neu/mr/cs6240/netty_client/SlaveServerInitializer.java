package neu.mr.cs6240.netty_client;

import org.apache.log4j.Logger;

import io.netty.channel.Channel;

/**
 * Adding encoders and decoders for SlaveServer
 * @author ajay subramanya
 * @author prasad
 */

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

public class SlaveServerInitializer extends ChannelInitializer<SocketChannel> {

	final Logger logger = Logger.getLogger(SlaveServerHandler.class);

	private static int numOfSlaves;
	private static String bucketName;
	private static Channel serverChannel;

	public SlaveServerInitializer(int numOfSlaves) {
		this.numOfSlaves = numOfSlaves;
	}

	public SlaveServerInitializer(int numOfSlaves, Channel serverChannel) {
		this.numOfSlaves = numOfSlaves;
		this.serverChannel = serverChannel;
	}

	public SlaveServerInitializer(int numOfSlaves, String bucketName, Channel serverChannel) {
		this.numOfSlaves = numOfSlaves;
		this.bucketName = bucketName;
		this.serverChannel = serverChannel;
	}

	@Override
	protected void initChannel(SocketChannel ch) throws Exception {

		logger.info("Initializing slave server: Number of slaves = " + numOfSlaves);
		ChannelPipeline pipeline = ch.pipeline();

		pipeline.addLast(new ObjectEncoder());
		pipeline.addLast(new ObjectDecoder(ClassResolvers.cacheDisabled(null)));

		// business logic.
		pipeline.addLast(new SlaveServerHandler(numOfSlaves, bucketName, serverChannel));

	}

}