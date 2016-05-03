package neu.mr.cs6240.netty;

import org.apache.log4j.Logger;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

/**
 * @author ajay subramanya
 */
public class ServerInitializer extends ChannelInitializer<SocketChannel> {

	final Logger logger = Logger.getLogger(Server.class);
	
	private int numberOfSlaves;
	
	ServerInitializer(int N) {		
		this.numberOfSlaves = N;
	}

	@Override
	protected void initChannel(SocketChannel ch) throws Exception {

		logger.info("Initilizing server: Number of slaves = " + numberOfSlaves);
		ChannelPipeline pipeline = ch.pipeline();

		pipeline.addLast(new StringDecoder());
		pipeline.addLast(new LengthFieldPrepender(4));
		pipeline.addLast(new StringEncoder());

		// business logic.
		pipeline.addLast(new ServerHandler(this.numberOfSlaves));
	}

}
