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
	private Map stormConf;

	public void update(Short houseId, String timeSlice, Double averageLoad) {

		// LOGGER.info("average for " + houseId + " at " + timeSlice + " is " +
		// averageLoad);
		if (averageLoadPerHousePerTimeSlice.containsKey(houseId)) {
			if (!averageLoadPerHousePerTimeSlice.get(houseId).containsKey(timeSlice)) {
				CircularList<Double> medianList = new CircularList<Double>(
						(Integer) stormConf.get("NUMBER_OF_DAYS_IN_ARCHIVE"));
				medianList.add(averageLoad);
				averageLoadPerHousePerTimeSlice.get(houseId).put(timeSlice, medianList);
			} else {
				CircularList<Double> medianList = averageLoadPerHousePerTimeSlice.get(houseId).get(
						timeSlice);
				medianList.add(averageLoad);
			}
		} else {

			ConcurrentHashMap<String, CircularList<Double>> bufferMap = new ConcurrentHashMap<String, CircularList<Double>>();
			Long bufferSize = (Long) stormConf.get("NUMBER_OF_DAYS_IN_ARCHIVE");

			CircularList<Double> medianList = new CircularList<Double>(bufferSize.intValue());
			medianList.add(averageLoad);
			bufferMap.put(timeSlice, medianList);
			averageLoadPerHousePerTimeSlice.put(houseId, bufferMap);

		}

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.stormConf = stormConf;
		cepConfig = new Configuration();
		cepConfig.getEngineDefaults().getThreading().setListenerDispatchPreserveOrder(false);
		cep = EPServiceProviderManager.getProvider("ArchiveMedianPerHouseBolt_" + this.hashCode(),
				cepConfig);
		cepConfig.addEventType("HistoryBean", HistoryBean.class.getName());
		cepRT = cep.getEPRuntime();
		cepAdm = cep.getEPAdministrator();

		EPStatement cepStatement = cepAdm.createEPL("@Hint('reclaim_group_aged="
				+ stormConf.get("dbLoadRate") + "')"
				+ "SELECT houseId,timeSlice,average FROM beans.HistoryBean"
				+ ".std:groupwin(houseId,timeSlice).win:expr_batch(averageLoad<0.0,false)"
				+ ".stat:weighted_avg(averageLoad,readingsCount) " + "group by houseId,timeSlice");
		cepStatement.setSubscriber(this);

		Config config = new Config();

		// Redisson will use load balance connections between listed servers
		config.addAddress(stormConf.get("redis.server") + ":6379");
		redisson = Redisson.create();
		averageLoadPerHousePerTimeSlice = redisson.getMap("query1a");

	}

	@Override
	public void execute(Tuple input) {
		HistoryBean bean = (HistoryBean) input.getValue(0);
		cepRT.sendEvent(bean);

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

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
