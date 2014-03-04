package spouts;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import streamers.ArchiveLoader;
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
	private Properties connectionProperties;
	private ScheduledExecutorService executor;
	private ConcurrentLinkedQueue<HistoryBean> archiveStreamBufferArr;
	private Long startTime;
	private int count = 0;
	private int dayCount;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		executor = Executors.newScheduledThreadPool(1);
		ScheduledFuture<?> future = executor.scheduleAtFixedRate(new ArchiveLoader<HistoryBean>(
				connectionProperties, archiveStreamBufferArr, (long) conf.get("SLICE_IN_MINUTES"),
				startTime, (String) conf.get("redis.server"), dayCount), 0, (long) conf
				.get("dbLoadRate"), TimeUnit.SECONDS);

	}

	/**
	 * Initialize the archive stream spout one for each day in the archive
	 * 
	 * @param archiveStreamBufferArr
	 * @param connectionProperties
	 * @param startTime
	 * @param dayCount
	 *            -- Archive spout count (starts from 1)
	 */
	public ArchiveStreamSpout(ConcurrentLinkedQueue<HistoryBean> archiveStreamBufferArr,
			Properties connectionProperties, Long startTime, int dayCount) {
		this.archiveStreamBufferArr = archiveStreamBufferArr;
		this.connectionProperties = connectionProperties;
		this.startTime = startTime;
		this.dayCount = dayCount;
	}

	@Override
	public void nextTuple() {
		while (true) {
			if (archiveStreamBufferArr.isEmpty()) {
				Utils.sleep(1);
				return;
			}

			@SuppressWarnings("unchecked")
			E obj = (E) archiveStreamBufferArr.poll();
			if (obj instanceof HistoryBean) {
				HistoryBean historyBean = (HistoryBean) obj;
				_collector.emit(new Values(historyBean, historyBean.getHouseId(), historyBean
						.getHouseholdId(), historyBean.getPlugId(), historyBean.getTimeSlice()));
				count++;
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
