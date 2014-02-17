package bolts;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.BufferUtils;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.log4j.Logger;

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
	private Map<Short, ConcurrentHashMap<String, Buffer>> averageLoadPerHousePerTimeSlice;
	private static transient final Logger LOGGER = Logger
			.getLogger(ArchiveMedianPerHouseBolt.class);
	private Map stormConf;

	public ArchiveMedianPerHouseBolt(
			Map<Short, ConcurrentHashMap<String, Buffer>> averageLoadPerHousePerTimeSlice) {
		this.averageLoadPerHousePerTimeSlice = averageLoadPerHousePerTimeSlice;

	}

	@SuppressWarnings("unchecked")
	public void update(Short houseId, String timeSlice, Double averageLoad) {

		LOGGER.info("average for " + houseId + " at " + timeSlice + " is " + averageLoad);
		if (averageLoadPerHousePerTimeSlice.containsKey(houseId)) {
			if (!averageLoadPerHousePerTimeSlice.get(houseId).containsKey(timeSlice)) {
				Buffer medianList = BufferUtils.synchronizedBuffer(new CircularFifoBuffer(
						(Integer) stormConf.get("NUMBER_OF_DAYS_IN_ARCHIVE")));
				medianList.add(averageLoad);
				averageLoadPerHousePerTimeSlice.get(houseId).put(timeSlice, medianList);
			} else {
				Buffer medianList = averageLoadPerHousePerTimeSlice.get(houseId).get(timeSlice);
				medianList.add(averageLoad);
			}
		} else {

			ConcurrentHashMap<String, Buffer> bufferMap = new ConcurrentHashMap<String, Buffer>();

			Buffer medianList = BufferUtils.synchronizedBuffer(new CircularFifoBuffer(
					(Integer) stormConf.get("NUMBER_OF_DAYS_IN_ARCHIVE")));
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
