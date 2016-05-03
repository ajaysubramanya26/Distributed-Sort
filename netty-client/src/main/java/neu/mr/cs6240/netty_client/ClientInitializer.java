package neu.mr.cs6240.netty_client;

import org.apache.log4j.Logger;

/**
 * Adds encoders and decoders for the Client channel
 * All messages are exchanged flowing through the pipeline
 * @author ajay subramanya
 * @author smitha
 */

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

public class ClientInitializer extends ChannelInitializer<SocketChannel> {

	final Logger logger = Logger.getLogger(ClientInitializer.class);

	final int MAX_FRAME_SIZE = 4 * 1024 * 1024;
	final int HEADER_LEN = 4;

	private int numOfSlaves;

	public ClientInitializer(int numOfSlaves) {
		this.numOfSlaves = numOfSlaves;
	}

	@Override
	protected void initChannel(SocketChannel ch) throws Exception {

		logger.info("Intitializing channel");

		ChannelPipeline pipeline = ch.pipeline();

		pipeline.addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME_SIZE, 0, HEADER_LEN, 0, HEADER_LEN));
		pipeline.addLast(new StringDecoder());
		pipeline.addLast(new StringEncoder());

		// business logic.
		pipeline.addLast(new ClientHandler(this.numOfSlaves));
	}

}
