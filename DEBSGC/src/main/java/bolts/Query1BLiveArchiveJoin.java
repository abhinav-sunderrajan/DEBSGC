package bolts;

import java.util.Calendar;
import java.util.Map;

import main.PlatformCore;

import org.apache.commons.collections.Buffer;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

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

	public Query1BLiveArchiveJoin(Fields outFields) {
		this.outFields = outFields;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		stats = new DescriptiveStatistics();
		_collector = collector;

	}

	@Override
	public void execute(Tuple input) {
		CurrentLoadPerPlugBean currentLoad = (CurrentLoadPerPlugBean) input.getValue(3);
		short houseId = currentLoad.getHouseId();
		short householdId = currentLoad.getHouseHoldId();
		short plugId = currentLoad.getPlugId();

		cal.setTimeInMillis(currentLoad.getCurrTime() + 2 * PlatformCore.SLICE_IN_MINUTES * 60
				* 1000);
		int hrs = cal.get(Calendar.HOUR);
		int mnts = cal.get(Calendar.MINUTE);
		String predTimeStart = String.format("%02d:%02d", hrs, mnts);
		cal.setTimeInMillis(currentLoad.getCurrTime() + 3 * PlatformCore.SLICE_IN_MINUTES * 60
				* 1000);
		hrs = cal.get(Calendar.HOUR);
		mnts = cal.get(Calendar.MINUTE);
		String predTimeEnd = String.format("%02d:%02d", hrs, mnts);

		String key = predTimeStart + " TO " + predTimeEnd;

		Buffer values = PlatformCore.averageLoadPerPlugPerTimeSlice.get(
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

			if (count % 200 == 0) {
				System.out.println("Predicted load at " + houseId + "_" + householdId + "_"
						+ plugId + " is " + predictedLoad + " for time " + key);
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
