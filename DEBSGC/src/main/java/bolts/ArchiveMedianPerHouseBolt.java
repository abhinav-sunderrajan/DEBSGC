package bolts;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.redisson.Config;
import org.redisson.Redisson;

import utils.CircularList;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import beans.HistoryBean;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

public class ArchiveMedianPerHouseBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private transient EPServiceProvider cep;
	private transient EPAdministrator cepAdm;
	private transient Configuration cepConfig;
	private transient EPRuntime cepRT;
	private Redisson redisson;
	private Map<Short, ConcurrentHashMap<String, CircularList<Double>>> averageLoadPerHousePerTimeSlice;
	private static final Logger LOGGER = Logger.getLogger(ArchiveMedianPerHouseBolt.class);
	private Long bufferSize;

	public void update(Short houseId, String timeSlice, Integer dayCount, Double averageLoad) {
		// LOGGER.info("average for " + houseId + " at day:" + dayCount +
		// " and time:" + timeSlice
		// + " is " + averageLoad);

		if (averageLoadPerHousePerTimeSlice.containsKey(houseId)) {
			if (!averageLoadPerHousePerTimeSlice.get(houseId).containsKey(timeSlice)) {
				CircularList<Double> medianList = new CircularList<Double>(bufferSize.intValue());
				medianList.add(averageLoad);
				ConcurrentHashMap<String, CircularList<Double>> bufferMap = averageLoadPerHousePerTimeSlice
						.get(houseId);
				bufferMap.put(timeSlice, medianList);
				averageLoadPerHousePerTimeSlice.put(houseId, bufferMap);

			} else {
				ConcurrentHashMap<String, CircularList<Double>> bufferMap = averageLoadPerHousePerTimeSlice
						.get(houseId);
				CircularList<Double> medianList = bufferMap.get(timeSlice);
				medianList.add(averageLoad);
				bufferMap.put(timeSlice, medianList);
				averageLoadPerHousePerTimeSlice.put(houseId, bufferMap);

			}
		} else {

			ConcurrentHashMap<String, CircularList<Double>> bufferMap = new ConcurrentHashMap<String, CircularList<Double>>();

			CircularList<Double> medianList = new CircularList<Double>(bufferSize.intValue());
			medianList.add(averageLoad);
			bufferMap.put(timeSlice, medianList);
			averageLoadPerHousePerTimeSlice.put(houseId, bufferMap);

		}

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		cepConfig = new Configuration();
		cepConfig.getEngineDefaults().getThreading().setListenerDispatchPreserveOrder(true);
		cep = EPServiceProviderManager.getProvider("ArchiveMedianPerHouseBolt_" + this.hashCode(),
				cepConfig);
		cepConfig.addEventType("HistoryBean", HistoryBean.class.getName());
		cepRT = cep.getEPRuntime();
		cepAdm = cep.getEPAdministrator();
		bufferSize = (Long) stormConf.get("NUMBER_OF_DAYS_IN_ARCHIVE");

		EPStatement cepStatement = cepAdm.createEPL("@Hint('reclaim_group_aged="
				+ stormConf.get("dbLoadRate") + "')"
				+ "SELECT houseId,timeSlice,dayCount,average FROM beans.HistoryBean"
				+ ".std:groupwin(houseId,timeSlice,dayCount).win:expr_batch(averageLoad<0.0,false)"
				+ ".stat:weighted_avg(averageLoad,readingsCount) "
				+ "group by houseId,timeSlice,dayCount");
		cepStatement.setSubscriber(this);

		Config config = new Config();

		// Redisson will use load balance connections between listed servers
		config.addAddress(stormConf.get("redis.server") + ":6379");
		redisson = Redisson.create(config);
		this.averageLoadPerHousePerTimeSlice = redisson.getMap("query1a");

	}

	@Override
	public void execute(Tuple input) {
		HistoryBean bean = (HistoryBean) input.getValue(0);
		cepRT.sendEvent(bean);

	}

	@Override
	public void cleanup() {
		redisson.flushdb();
		redisson.shutdown();

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
