package bolts;

import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.log4j.Logger;
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
import beans.CurrentLoadPerPlugBean;

public class Query1BLiveArchiveJoin implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Calendar cal = Calendar.getInstance();
	private OutputCollector _collector;
	private transient DescriptiveStatistics stats;
	private Fields outFields;
	private static int count = 0;
	private Map stormConf;
	private Redisson redisson;
	private Long prevTime;
	private Long sliceInMin;
	private String prevKey;
	private Map<String, ConcurrentHashMap<String, CircularList<Double>>> averageLoadPerPlugPerTimeSlice;
	private static final Logger LOGGER = Logger.getLogger(Query1BLiveArchiveJoin.class);

	public Query1BLiveArchiveJoin(Fields outFields) {
		this.outFields = outFields;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		stats = new DescriptiveStatistics();
		_collector = collector;
		this.stormConf = stormConf;
		Config config = new Config();
		config.setConnectionPoolSize(2);
		sliceInMin = (Long) stormConf.get("SLICE_IN_MINUTES");
		// Redisson will use load balance connections between listed servers
		config.addAddress(stormConf.get("redis.server") + ":6379");
		redisson = Redisson.create(config);
		averageLoadPerPlugPerTimeSlice = redisson.getMap("query1b");

		prevTime = Long.parseLong((String) stormConf.get("live.start.time"));
		cal.setTimeInMillis(prevTime + 2 * sliceInMin * 60 * 1000);
		int hrs = cal.get(Calendar.HOUR);
		int mnts = cal.get(Calendar.MINUTE);
		String predTimeStart = String.format("%02d:%02d", hrs, mnts);
		cal.setTimeInMillis(prevTime + 3 * sliceInMin * 60 * 1000);
		hrs = cal.get(Calendar.HOUR);
		mnts = cal.get(Calendar.MINUTE);
		String predTimeEnd = String.format("%02d:%02d", hrs, mnts);

		prevKey = predTimeStart + " TO " + predTimeEnd;

	}

	@Override
	public void execute(Tuple input) {
		String key;
		CurrentLoadPerPlugBean currentLoad = (CurrentLoadPerPlugBean) input.getValue(3);
		Long time = currentLoad.getCurrTime();
		short houseId = currentLoad.getHouseId();
		short householdId = currentLoad.getHouseHoldId();
		short plugId = currentLoad.getPlugId();

		if (time - prevTime < sliceInMin * 60 * 1000) {
			key = prevKey;
		} else {
			cal.setTimeInMillis(time + 2 * sliceInMin * 60 * 1000);
			int hrs = cal.get(Calendar.HOUR);
			int mnts = cal.get(Calendar.MINUTE);
			String predTimeStart = String.format("%02d:%02d", hrs, mnts);
			cal.setTimeInMillis(time + 3 * sliceInMin * 60 * 1000);
			hrs = cal.get(Calendar.HOUR);
			mnts = cal.get(Calendar.MINUTE);
			String predTimeEnd = String.format("%02d:%02d", hrs, mnts);

			key = predTimeStart + " TO " + predTimeEnd;
			prevTime = time;
			prevKey = key;

		}

		CircularList<Double> values = averageLoadPerPlugPerTimeSlice.get(
				houseId + "_" + householdId + "_" + plugId).get(key);

		if (values != null) {

			for (Object obj : values) {
				stats.addValue((Double) obj);
			}

			double predictedLoad = (stats.getPercentile(50) + currentLoad.getCurrentAverageLoad()) / 2.0;
			_collector.emit(new Values(houseId + "_" + householdId + "_" + plugId, currentLoad
					.getCurrentAverageLoad(), predictedLoad, key, currentLoad.getEvaluationTime()));
			stats.clear();
			count++;

			// Not sure how to visualize this hence logging the predicted load
			// for every 1000 tuples processed.
			// if (count % 1000 == 0) {
			// LOGGER.info("Predicted load at " + houseId + "_" + householdId +
			// "_" + plugId
			// + " is " + predictedLoad + " for time " + key);
			// }
		}

	}

	@Override
	public void cleanup() {
		redisson.flushdb();
		redisson.shutdown();

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(outFields);

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
