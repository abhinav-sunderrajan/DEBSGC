package bolts;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import utils.OutputDF;
import utils.VisualizationServerConnect;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

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
	private static final Logger LOGGER = Logger.getLogger(StreamProviderBolt.class);

	public StreamProviderBolt(String serverIP, int serverPort) {
		this.serverIP = serverIP;
		this.serverPort = serverPort;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		try {
			VisualizationServerConnect<OutputDF> send = new VisualizationServerConnect<OutputDF>(
					serverIP);
			EventHandler<OutputDF> handler = send.connectToNettyServer(serverPort);

			Disruptor<OutputDF> disruptor = new Disruptor<OutputDF>(OutputDF.EVENT_FACTORY,
					EXECUTOR, new SingleThreadedClaimStrategy(RING_SIZE),
					new SleepingWaitStrategy());
			disruptor.handleEventsWith(handler);
			ringBuffer = disruptor.start();
		} catch (InterruptedException e) {
			LOGGER.error("Error connecting to stream subscriber", e);
		}

	}

}
