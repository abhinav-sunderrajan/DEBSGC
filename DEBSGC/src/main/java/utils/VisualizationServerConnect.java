package utils;

import java.net.InetSocketAddress;
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

import com.lmax.disruptor.EventHandler;

/**
 * 
 * This class is responsible for establishing a connection to the
 * visualization-server
 * 
 */
public class VisualizationServerConnect<E> {

	private String serverAddr;
	private ClientBootstrap bootstrap;
	private ChannelFuture future;
	private ChannelFactory factory;
	protected EventHandler<E> handler;
	private static final Logger LOGGER = Logger.getLogger(VisualizationServerConnect.class);

	/**
	 * 
	 * @param serverIP
	 * @param buffer
	 * @param executor
	 * @param streamRate
	 */
	public VisualizationServerConnect(String serverIP) {
		this.serverAddr = serverIP;
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
	public EventHandler<E> connectToNettyServer(final int serverPort) throws InterruptedException {
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
		// Wait for a couple of seconds for the handler to be initialized before
		// return
		Thread.sleep(2000);
		return handler;

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

					handler = new EventHandler<E>() {

						public void onEvent(E obj, final long sequence, final boolean endOfBatch)
								throws Exception {
							responseEvent = new UpstreamMessageEvent(channel, obj,
									channel.getRemoteAddress());
							context.sendUpstream(responseEvent);

						}
					};

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
