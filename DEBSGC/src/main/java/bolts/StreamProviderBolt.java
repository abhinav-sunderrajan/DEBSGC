package bolts;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import utils.NettyServerConnect;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;

public abstract class StreamProviderBolt<E> implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected ConcurrentLinkedQueue<E> buffer;
	private String serverIP;
	private int serverPort;
	private static final Logger LOGGER = Logger.getLogger(StreamProviderBolt.class);

	public StreamProviderBolt(String serverIP, int serverPort) {
		this.buffer = new ConcurrentLinkedQueue<E>();
		this.serverIP = serverIP;
		this.serverPort = serverPort;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		NettyServerConnect<E> send = new NettyServerConnect<E>(serverIP, buffer);
		try {
			send.connectToNettyServer(serverPort);
		} catch (InterruptedException e) {
			LOGGER.error("Error connecting to stream subscriber", e);
		}

	}

}
