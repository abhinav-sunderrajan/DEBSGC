package bolts;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.redisson.Config;
import org.redisson.Redisson;

import utils.CircularList;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import beans.HistoryBean;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

public class ArchiveMedianPerPlugBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector _collector;
	private Fields outputFields;
	private Redisson redisson;
	private Map<String, ConcurrentHashMap<String, CircularList<Double>>> averageLoadPerPlugPerTimeSlice;
	private Map stormConf;
	private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();
	private transient Disruptor<HistoryBean> disruptor;
	private static RingBuffer<HistoryBean> ringBuffer;
	private final static int RING_SIZE = 32768;
	private static EventHandler<HistoryBean> handler;

	/**
	 * Initialize with a declaration of the output fields for clarity while
	 * configuring topology.
	 * 
	 * @param outputFields
	 */
	public ArchiveMedianPerPlugBolt(Fields outputFields) {
		this.outputFields = outputFields;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void prepare(final Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		this.stormConf = stormConf;
		Config config = new Config();

		// Redisson will use load balance connections between listed servers
		config.addAddress(stormConf.get("redis.server") + ":6379");
		redisson = Redisson.create(config);
		averageLoadPerPlugPerTimeSlice = redisson.getMap("query1b");

		disruptor = new Disruptor<HistoryBean>(HistoryBean.EVENT_FACTORY, EXECUTOR,
				new SingleThreadedClaimStrategy(RING_SIZE), new SleepingWaitStrategy());
		disruptor.handleEventsWith(handler);
		ringBuffer = disruptor.start();

		implementLMAXHandler();

	}

	@Override
	public void execute(Tuple input) {

		HistoryBean bean = (HistoryBean) input.getValue(0);
		if (bean.getHouseholdId() != -1) {
			final short houseId = input.getShort(1);
			String timeSlice = input.getString(4);

			_collector.emit(new Values(bean, houseId, timeSlice));
			long sequence = ringBuffer.next();
			HistoryBean next = ringBuffer.get(sequence);
			next.setAverageLoad(bean.getAverageLoad());
			next.setHouseholdId(bean.getHouseholdId());
			next.setHouseId(houseId);
			next.setPlugId(bean.getPlugId());
			next.setReadingsCount(bean.getReadingsCount());
			next.setTimeSlice(timeSlice);
			ringBuffer.publish(sequence);

		} else {
			Utils.sleep(1000);
			_collector.emit(new Values(bean, bean.getHouseId(), bean.getTimeSlice()));

		}

	}

	@Override
	public void cleanup() {
		redisson.flushdb();
		redisson.shutdown();
		disruptor.shutdown();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(this.outputFields);

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	private void implementLMAXHandler() {
		handler = new EventHandler<HistoryBean>() {
			public void onEvent(final HistoryBean bean, final long sequence,
					final boolean endOfBatch) throws Exception {
				String key = bean.getHouseId() + "_" + bean.getHouseholdId() + "_"
						+ bean.getPlugId();

				if (averageLoadPerPlugPerTimeSlice.containsKey(key)) {

					if (!averageLoadPerPlugPerTimeSlice.get(key).containsKey(bean.getTimeSlice())) {
						Long size = (Long) stormConf.get("NUMBER_OF_DAYS_IN_ARCHIVE");
						CircularList<Double> medianList = new CircularList<Double>(size.intValue());
						medianList.add((double) bean.getAverageLoad());
						ConcurrentHashMap<String, CircularList<Double>> buffermap = averageLoadPerPlugPerTimeSlice
								.get(key);
						buffermap.put(bean.getTimeSlice(), medianList);
						averageLoadPerPlugPerTimeSlice.put(key, buffermap);
					} else {
						ConcurrentHashMap<String, CircularList<Double>> buffermap = averageLoadPerPlugPerTimeSlice
								.get(key);
						CircularList<Double> medianList = buffermap.get(bean.getTimeSlice());
						medianList.add((double) bean.getAverageLoad());
						buffermap.put(bean.getTimeSlice(), medianList);
						averageLoadPerPlugPerTimeSlice.put(key, buffermap);

					}

				} else {

					ConcurrentHashMap<String, CircularList<Double>> bufferMap = new ConcurrentHashMap<String, CircularList<Double>>();
					Long bufferSize = (Long) stormConf.get("NUMBER_OF_DAYS_IN_ARCHIVE");
					CircularList<Double> medianList = new CircularList<Double>(
							bufferSize.intValue());
					medianList.add((double) bean.getAverageLoad());
					bufferMap.put(bean.getTimeSlice(), medianList);
					averageLoadPerPlugPerTimeSlice.put(key, bufferMap);
				}

			}
		};

	}

}
