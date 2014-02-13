package bolts;

import java.sql.Timestamp;
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
import beans.CurrentLoadPerHouseBean;

public class Query1ALiveArchiveJoin implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Calendar cal = Calendar.getInstance();
	private OutputCollector _collector;
	private transient DescriptiveStatistics stats;
	private Fields outFields;
	private static int count = 0;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		stats = new DescriptiveStatistics();

	}

	public Query1ALiveArchiveJoin(Fields outFields) {
		this.outFields = outFields;
	}

	@Override
	public void execute(Tuple input) {

		CurrentLoadPerHouseBean currentLoad = (CurrentLoadPerHouseBean) input.getValue(1);
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
		Buffer values = PlatformCore.averageLoadPerHousePerTimeSlice.get(currentLoad.getHouseId())
				.get(key);

		if (values != null) {
			for (Object obj : values) {
				stats.addValue((double) obj);
			}
			_collector.emit(new Values(currentLoad.getHouseId(), currentLoad
					.getCurrentAverageLoad(), (stats.getPercentile(50) + currentLoad
					.getCurrentAverageLoad()) / 2.0, key, currentLoad.getEvaluationTime()));
			stats.clear();

		}

		count++;
		if (count % 1000 == 0) {
			System.out.println(new Timestamp(currentLoad.getCurrTime()));
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
