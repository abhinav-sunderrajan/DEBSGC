package bolts;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import main.PlatformCore;

import org.apache.log4j.Logger;

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
	private static int count = 0;
	private static final Logger LOGGER = Logger.getLogger(Query2AOutlierEstimator.class);

	public Query2AOutlierEstimator(Fields fields) {
		outFields = fields;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		df = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");

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

		ConcurrentHashMap<String, Boolean> plugIdMap;
		boolean isgreater = (medianLoad > globalMedian) ? true : false;

		if (PlatformCore.loadStatusMap.containsKey(houseId.shortValue())) {
			plugIdMap = PlatformCore.loadStatusMap.get(houseId.shortValue());
			if (plugIdMap.containsKey(householdId + "-" + plugId)) {

				// Calculate change in percentage if and only if there is a
				// change.
				if (plugIdMap.get(householdId + "-" + plugId) != isgreater) {
					// Update the change before calculating the percentage
					// change.
					plugIdMap.put(householdId + "-" + plugId, isgreater);
					double percentage = calculatePercentagePlugsGreater(plugIdMap);
					_collector.emit(new Values(timeFrame, houseId, percentage, (System
							.currentTimeMillis() - queryEvalTime)));
				} else {
					_collector.emit(new Values(timeFrame, null, null, null));
				}
			} else {
				plugIdMap.put(householdId + "-" + plugId, isgreater);
				double percentage = calculatePercentagePlugsGreater(plugIdMap);
				_collector.emit(new Values(timeFrame, houseId, percentage, (System
						.currentTimeMillis() - queryEvalTime)));
			}
		} else {
			plugIdMap = new ConcurrentHashMap<String, Boolean>();
			plugIdMap.put(householdId + "-" + plugId, isgreater);
			PlatformCore.loadStatusMap.put(houseId.shortValue(), plugIdMap);
			if (isgreater) {
				_collector.emit(new Values(timeFrame, houseId, 100.0,
						(System.currentTimeMillis() - queryEvalTime)));
			} else {
				_collector.emit(new Values(timeFrame, null, null, null));
			}
		}

	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(outFields);

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	private double calculatePercentagePlugsGreater(ConcurrentHashMap<String, Boolean> plugIdMap) {

		int above = 0;
		int below = 0;

		for (Boolean value : plugIdMap.values()) {
			if (value) {
				above++;
			} else {
				below++;
			}
		}

		return ((above * 100.0) / (below + above));

	}

}
