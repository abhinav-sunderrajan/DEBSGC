package spouts;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import beans.HistoryBean;

/**
 * Emits average load per plug, per household, per house
 * 
 * @author abhinav
 * 
 * @param <E>
 */
public class ArchiveStreamSpout<E> extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	private Queue<E> sharedBuffer;
	private SpoutOutputCollector _collector;
	private static final boolean _isDistributed = false;

	public ArchiveStreamSpout(final ConcurrentLinkedQueue<E> buffer, int archiveStreamRate) {
		this.sharedBuffer = buffer;

	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;

	}

	@Override
	public void nextTuple() {
		if (sharedBuffer.isEmpty()) {
			Utils.sleep(500);
			return;
		}

		E obj = sharedBuffer.poll();
		if (obj instanceof HistoryBean) {
			HistoryBean bean = (HistoryBean) obj;
			_collector.emit(new Values(bean, bean.getHouseId(), bean.getHouseholdId(), bean
					.getPlugId(), bean.getTimeSlice()));
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("HistoryBean", "houseId", "householdId", "plugId", "timeSlice"));
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
