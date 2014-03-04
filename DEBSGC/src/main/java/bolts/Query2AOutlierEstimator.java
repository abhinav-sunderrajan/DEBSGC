package bolts;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;
import org.redisson.Config;
import org.redisson.Redisson;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class Query2AOutlierEstimator implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector _collector;
	private Fields outFields;
	private static Calendar cal = Calendar.getInstance();
	private static DateFormat df;
	private Redisson redisson;
	private ConcurrentMap<Integer, ConcurrentMap<String, Boolean>> houseIdMap;
	private static final Logger LOGGER = Logger.getLogger(Query2AOutlierEstimator.class);

	/**
	 * 
	 * @param fields
	 *            output fields
	 */
	public Query2AOutlierEstimator(Fields fields) {
		outFields = fields;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		df = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
		Config config = new Config();
		config.setConnectionPoolSize(2);

		// Redisson will use load balance connections between listed servers
		config.addAddress(stormConf.get("redis.server") + ":6379");
		redisson = Redisson.create(config);
		houseIdMap = redisson.getMap("query2a");

	}

	@Override
	public void execute(Tuple input) {

		Double medianLoad = input.getDouble(0);
		Double globalMedian = input.getDouble(1);
		Long timestampStart = input.getLong(2);
		Long timestampEnd = input.getLong(3);
		Long queryEvalTime = input.getLong(4);
		Integer houseId = input.getInteger(5);
		Integer householdId = input.getInteger(6);
		Integer plugId = input.getInteger(7);

		cal.setTimeInMillis(timestampStart);

		String timeStart = df.format(cal.getTime());
		cal.setTimeInMillis(timestampEnd);

		String timeEnd = df.format(cal.getTime());
		String timeFrame = timeStart + " TO " + timeEnd;

		Boolean isgreater = (medianLoad > globalMedian) ? true : false;

		if (houseIdMap.containsKey(houseId)) {

			ConcurrentMap<String, Boolean> plugIdMap = houseIdMap.get(houseId);

			if (plugIdMap.containsKey(householdId + "-" + plugId)) {
				Boolean value = plugIdMap.get(householdId + "-" + plugId);
				if (value.compareTo(isgreater) != 0) {
					// Update the change before calculating the percentage
					// change.
					plugIdMap.put(householdId + "-" + plugId, isgreater);
					double percentage = calculatePercentagePlugsGreater(plugIdMap);
					// Put the updated value back
					houseIdMap.put(houseId, plugIdMap);
					_collector.emit(new Values(timeFrame, houseId, percentage, (System
							.currentTimeMillis() - queryEvalTime)));
				} else {
					_collector.emit(new Values(timeFrame, null, null,
							(System.currentTimeMillis() - queryEvalTime)));
				}
			} else {
				plugIdMap.put(householdId + "-" + plugId, isgreater);
				double percentage = calculatePercentagePlugsGreater(plugIdMap);
				// Put the updated value back
				houseIdMap.put(houseId, plugIdMap);
				_collector.emit(new Values(timeFrame, houseId, percentage, (System
						.currentTimeMillis() - queryEvalTime)));

			}

		} else {

			ConcurrentMap<String, Boolean> plugIdMap = new ConcurrentHashMap<String, Boolean>();
			plugIdMap.put(householdId + "-" + plugId, isgreater);
			houseIdMap.put(houseId, plugIdMap);
			if (isgreater) {
				_collector.emit(new Values(timeFrame, houseId, 100.0,
						(System.currentTimeMillis() - queryEvalTime)));
			} else {
				_collector.emit(new Values(timeFrame, null, null,
						(System.currentTimeMillis() - queryEvalTime)));
			}

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
		return null;
	}

	private double calculatePercentagePlugsGreater(Map<String, Boolean> map) {

		int above = 0;
		int below = 0;

		for (boolean value : map.values()) {
			if (value) {
				above++;
			} else {
				below++;
			}
		}

		return ((above * 100.0) / (below + above));

	}

}
