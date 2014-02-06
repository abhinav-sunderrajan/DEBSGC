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
	private int propertyFilterId;
	private static final boolean _isDistributed = false;

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
	 */
	public LiveStreamSpout(final ConcurrentLinkedQueue<E> buffer, final Object monitor,
			final ScheduledExecutorService executor, final int streamRate, final int port,
			final String writeFileDir, final String imageSaveDirectory, int property) {
		this.buffer = buffer;
		this.monitor = monitor;
		this.propertyFilterId = property;

		// Fire up the netty server to listen to streams at the given port.
		NettyServer<E> server = new NettyServer<E>((ConcurrentLinkedQueue<E>) buffer, streamRate,
				writeFileDir, imageSaveDirectory);
		server.listen(port);

	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;

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
				// If bean is a punctuation indicating the end of
				if (bean.getId() == -1) {

				}

				if (bean.getProperty() == propertyFilterId)
					_collector.emit(new Values(bean, bean.getHouseId(), bean.getHouseholdId(), bean
							.getPlugId()));
			}
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("livebean", "houseId", "householdId", "plugId"));

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
