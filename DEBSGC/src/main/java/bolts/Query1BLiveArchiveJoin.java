package bolts;

import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.Buffer;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.log4j.Logger;

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
	private Map<String, ConcurrentHashMap<String, Buffer>> averageLoadPerPlugPerTimeSlice;
	private static final Logger LOGGER = Logger.getLogger(Query1BLiveArchiveJoin.class);

	public Query1BLiveArchiveJoin(Fields outFields,
			Map<String, ConcurrentHashMap<String, Buffer>> averageLoadPerPlugPerTimeSlice) {
		this.outFields = outFields;
		this.averageLoadPerPlugPerTimeSlice = averageLoadPerPlugPerTimeSlice;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		stats = new DescriptiveStatistics();
		_collector = collector;
		this.stormConf = stormConf;

	}

	@Override
	public void execute(Tuple input) {
		CurrentLoadPerPlugBean currentLoad = (CurrentLoadPerPlugBean) input.getValue(3);
		short houseId = currentLoad.getHouseId();
		short householdId = currentLoad.getHouseHoldId();
		short plugId = currentLoad.getPlugId();

		cal.setTimeInMillis(currentLoad.getCurrTime() + 2
				* (Integer) stormConf.get("SLICE_IN_MINUTES") * 60 * 1000);
		int hrs = cal.get(Calendar.HOUR);
		int mnts = cal.get(Calendar.MINUTE);
		String predTimeStart = String.format("%02d:%02d", hrs, mnts);
		cal.setTimeInMillis(currentLoad.getCurrTime() + 3
				* (Integer) stormConf.get("SLICE_IN_MINUTES") * 60 * 1000);
		hrs = cal.get(Calendar.HOUR);
		mnts = cal.get(Calendar.MINUTE);
		String predTimeEnd = String.format("%02d:%02d", hrs, mnts);

		String key = predTimeStart + " TO " + predTimeEnd;

		Buffer values = this.averageLoadPerPlugPerTimeSlice.get(
				houseId + "_" + householdId + "_" + plugId).get(key);

		if (values != null) {

			for (Object obj : values) {
				stats.addValue((Float) obj);
			}

			double predictedLoad = (stats.getPercentile(50) + currentLoad.getCurrentAverageLoad()) / 2.0;
			_collector.emit(new Values(houseId + "_" + householdId + "_" + plugId, currentLoad
					.getCurrentAverageLoad(), predictedLoad, key, currentLoad.getEvaluationTime()));
			stats.clear();
			count++;

			// Not sure how to visualize this hence logging the predicted load
			// for every 200 tuples processed.
			if (count % 200 == 0) {
				LOGGER.info("Predicted load at " + houseId + "_" + householdId + "_" + plugId
						+ " is " + predictedLoad + " for time " + key);
			}
		}

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

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
