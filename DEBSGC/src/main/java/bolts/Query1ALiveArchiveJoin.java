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
	private Map stormConf;
	private Redisson redisson;
	private Map<Short, ConcurrentHashMap<String, CircularList<Double>>> averageLoadPerHousePerTimeSlice;
	private static final Logger LOGGER = Logger.getLogger(Query1ALiveArchiveJoin.class);

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		this.stormConf = stormConf;
		stats = new DescriptiveStatistics();
		Config config = new Config();
		config.setConnectionPoolSize(2);

		// Redisson will use load balance connections between listed servers
		config.addAddress(stormConf.get("redis.server") + ":6379");
		redisson = Redisson.create(config);
		averageLoadPerHousePerTimeSlice = redisson.getMap("query1a");

	}

	public Query1ALiveArchiveJoin(Fields outFields) {
		this.outFields = outFields;
	}

	@Override
	public void execute(Tuple input) {

		CurrentLoadPerHouseBean currentLoad = (CurrentLoadPerHouseBean) input.getValue(1);
		Long sliceInMin = (Long) stormConf.get("SLICE_IN_MINUTES");
		cal.setTimeInMillis(currentLoad.getCurrTime() + 2 * sliceInMin * 60 * 1000);
		int hrs = cal.get(Calendar.HOUR);
		int mnts = cal.get(Calendar.MINUTE);
		String predTimeStart = String.format("%02d:%02d", hrs, mnts);
		cal.setTimeInMillis(currentLoad.getCurrTime() + 3 * sliceInMin * 60 * 1000);
		hrs = cal.get(Calendar.HOUR);
		mnts = cal.get(Calendar.MINUTE);
		String predTimeEnd = String.format("%02d:%02d", hrs, mnts);

		String key = predTimeStart + " TO " + predTimeEnd;
		CircularList<Double> values = averageLoadPerHousePerTimeSlice.get(currentLoad.getHouseId())
				.get(key);

		if (values != null) {
			for (Object obj : values) {
				stats.addValue((Double) obj);
			}
			_collector.emit(new Values(currentLoad.getHouseId(), currentLoad
					.getCurrentAverageLoad(), (stats.getPercentile(50) + currentLoad
					.getCurrentAverageLoad()) / 2.0, key, currentLoad.getEvaluationTime()));
			stats.clear();

		}

		count++;
		if (count % 1000 == 0) {
			LOGGER.info(key + " " + values);
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
