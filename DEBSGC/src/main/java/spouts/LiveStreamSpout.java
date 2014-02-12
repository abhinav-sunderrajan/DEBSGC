package spouts;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;

import utils.NettyServer;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import beans.SmartPlugBean;

/**
 * The live stream spout reads data ingested by the netty server subscribing to
 * the live stream. Noted that the spout is always non-distributed.
 * 
 * @author abhinav
 * 
 * @param <E>
 */
public class LiveStreamSpout<E> extends BaseRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Object monitor;
	private Queue<E> buffer;
	private SpoutOutputCollector _collector;
	private static final boolean _isDistributed = false;
	private Fields outFields;
	private int streamRate;
	private String writeFileDir;
	private String imageSaveDirectory;
	private int port;

	/**
	 * 
	 * @param buffer
	 * @param monitor
	 * @param executor
	 * @param streamRate
	 * @param df
	 * @param port
	 * @param writeFileDir
	 * @param imageSaveDirectory
	 * @param property
	 * @param outFields
	 */
	public LiveStreamSpout(final ConcurrentLinkedQueue<E> buffer, final Object monitor,
			final ScheduledExecutorService executor, final int streamRate, final int port,
			final String writeFileDir, final String imageSaveDirectory, final Fields outFields) {
		this.buffer = buffer;
		this.monitor = monitor;
		this.outFields = outFields;
		this.streamRate = streamRate;
		this.writeFileDir = writeFileDir;
		this.port = port;
		this.imageSaveDirectory = imageSaveDirectory;

	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;

		// Fire up the netty server to listen to streams at the given port.
		NettyServer<E> server = new NettyServer<E>((ConcurrentLinkedQueue<E>) buffer, streamRate,
				writeFileDir, imageSaveDirectory);
		server.listen(port);

	}

	@Override
	public void nextTuple() {
		while (true) {

			if (buffer.isEmpty()) {
				return;
			}
			synchronized (monitor) {
				monitor.notifyAll();
			}
			E obj = buffer.poll();
			if (obj instanceof SmartPlugBean) {
				SmartPlugBean bean = (SmartPlugBean) obj;
				_collector.emit(new Values(bean, bean.getHouseId(), bean.getHouseholdId(), bean
						.getPlugId()));
			}
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(outFields);

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		if (!_isDistributed) {
			Map<String, Object> ret = new HashMap<String, Object>();
			ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
			return ret;
		} else {
			return null;
		}
	}

}