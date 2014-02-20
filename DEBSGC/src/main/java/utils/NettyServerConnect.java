package utils;

import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.DownstreamMessageEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.UpstreamMessageEvent;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ClassResolvers;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;

/**
 * 
 * This class is responsible for establishing a connection to the server
 * 
 */
public class NettyServerConnect<E> {

	private String serverAddr;
	private ClientBootstrap bootstrap;
	private ChannelFuture future;
	private Queue<E> buffer;
	private ChannelFactory factory;
	private static final Logger LOGGER = Logger.getLogger(NettyServerConnect.class);

	/**
	 * 
	 * @param serverIP
	 * @param buffer
	 * @param executor
	 * @param streamRate
	 */
	public NettyServerConnect(String serverIP, final ConcurrentLinkedQueue<E> buffer) {
		this.serverAddr = serverIP;
		this.buffer = buffer;
		this.factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool());
	}

	/**
	 * Call method to establish connection with server with a timeout of 10
	 * seconds.
	 * 
	 * @param serverPort
	 * @param buffer
	 * @throws InterruptedException
	 */
	public void connectToNettyServer(final int serverPort) throws InterruptedException {
		bootstrap = new ClientBootstrap(factory);
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("keepAlive", true);
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() {
				return Channels.pipeline(
						new ObjectEncoder(),
						new ObjectDecoder(ClassResolvers.cacheDisabled(getClass().getClassLoader())),
						new SendUpStream(), new MessageSender());
			}
		});
		future = bootstrap.connect(new InetSocketAddress(serverAddr, serverPort));
		future.await(10, TimeUnit.SECONDS);
		LOGGER.info("Connected to server");
		Channel channel = future.getChannel();
		channel.write(new String("hello server"));

	}

	/**
	 * 
	 * Send live data to the server.
	 * 
	 */

	private class SendUpStream extends SimpleChannelHandler {
		private Channel channel;
		ChannelHandlerContext context;
		private ChannelEvent responseEvent;

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
			if (e.getMessage() instanceof String) {
				String msg = (String) e.getMessage();
				if (msg.equalsIgnoreCase("hello client")) {
					channel = e.getChannel();
					context = ctx;

					while (true) {
						while (buffer.isEmpty()) {
							// Poll till the producer has filled the queue. Bad
							// approach
							// will
							// optimize this.
						}
						E obj = buffer.poll();
						responseEvent = new UpstreamMessageEvent(channel, obj,
								channel.getRemoteAddress());
						context.sendUpstream(responseEvent);
					}

				}
			}

		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
			e.getCause().printStackTrace();
			e.getChannel().close();
		}
	}

	private class MessageSender extends SimpleChannelHandler {

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

			Channel channel = e.getChannel();
			ChannelFuture channelFuture = Channels.future(e.getChannel());
			ChannelEvent responseEvent;
			Object bean = e.getMessage();
			responseEvent = new DownstreamMessageEvent(channel, channelFuture, bean,
					channel.getRemoteAddress());
			ctx.sendDownstream(responseEvent);

		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
			e.getCause().printStackTrace();
			e.getChannel().close();
		}
	}

}
