package spouts;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
	private SpoutOutputCollector _collector;
	private static final boolean _isDistributed = false;
	private int queueIndex;
	private List<ConcurrentLinkedQueue<HistoryBean>> archiveStreamBufferArr;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;

	}

	public ArchiveStreamSpout(int queueIndex,
			List<ConcurrentLinkedQueue<HistoryBean>> archiveStreamBufferArr) {
		this.queueIndex = queueIndex;
		this.archiveStreamBufferArr = archiveStreamBufferArr;
	}

	@Override
	public void nextTuple() {
		while (true) {
			if (archiveStreamBufferArr.get(queueIndex).isEmpty()) {
				Utils.sleep(500);
				return;
			}

			E obj = (E) archiveStreamBufferArr.get(queueIndex).poll();
			if (obj instanceof HistoryBean) {
				HistoryBean historyBean = (HistoryBean) obj;
				_collector.emit(new Values(historyBean, historyBean.getHouseId(), historyBean
						.getHouseholdId(), historyBean.getPlugId(), historyBean.getTimeSlice()));
			}
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
