package bolts;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import utils.OutputDF;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;

import com.lmax.disruptor.RingBuffer;

/**
 * Abstract class is the interface between the data-stream processing platform
 * and the outside world through a Netty socket which connects to a server.
 * 
 * @author abhinav
 * 
 */
public abstract class StreamProviderBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String serverIP;
	private int serverPort;
	protected static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();
	protected static RingBuffer<OutputDF> ringBuffer;
	protected final static int RING_SIZE = 262144;
	protected String localhost;
	protected transient Runtime runtime;
	private static final Logger LOGGER = Logger.getLogger(StreamProviderBolt.class);

	/**
	 * Initialize with the server host and port to which to send the processed
	 * streams to.
	 * 
	 * @param serverIP
	 * @param serverPort
	 */
	public StreamProviderBolt(String serverIP, int serverPort) {
		this.serverIP = serverIP;
		this.serverPort = serverPort;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		try {
			// VisualizationServerConnect<OutputDF> send = new
			// VisualizationServerConnect<OutputDF>(
			// serverIP);
			// EventHandler<OutputDF> handler =
			// send.connectToNettyServer(serverPort);
			//
			// Disruptor<OutputDF> disruptor = new
			// Disruptor<OutputDF>(OutputDF.EVENT_FACTORY,
			// EXECUTOR, new SingleThreadedClaimStrategy(RING_SIZE),
			// new SleepingWaitStrategy());
			// disruptor.handleEventsWith(handler);
			// ringBuffer = disruptor.start();
			localhost = InetAddress.getLocalHost().getHostName();
			runtime = Runtime.getRuntime();
			LOGGER.info("TOTAL MEMORY:" + (runtime.totalMemory() / (1024 * 1024)) + "MB");

		} catch (UnknownHostException e) {
			LOGGER.error("Error detrmining local-host address");
		}

	}
}
