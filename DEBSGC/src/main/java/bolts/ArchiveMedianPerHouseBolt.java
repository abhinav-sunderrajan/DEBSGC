package bolts;

import java.util.Map;

import main.PlatformCore;

import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.BufferUtils;
import org.apache.commons.collections.buffer.CircularFifoBuffer;

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
	private EPServiceProvider cep;
	private EPAdministrator cepAdm;
	private Configuration cepConfig;
	private EPRuntime cepRT;
	private OutputCollector _collector;

	public void update(Float averageLoad, Short houseId, String timeSlice) {
		if (!PlatformCore.averageLoadPerHousePerTimeSlice.get(houseId).containsKey(timeSlice)) {
			Buffer medianList = BufferUtils.synchronizedBuffer(new CircularFifoBuffer(
					PlatformCore.NUMBER_OF_ARCHIVE_STREAMS));
			medianList.add(averageLoad);
			PlatformCore.averageLoadPerHousePerTimeSlice.get(houseId).put(timeSlice, medianList);
		} else {
			Buffer medianList = PlatformCore.averageLoadPerHousePerTimeSlice.get(houseId).get(
					timeSlice);
			medianList.add(averageLoad);
		}

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		cepConfig = new Configuration();
		cepConfig.getEngineDefaults().getThreading().setListenerDispatchPreserveOrder(false);
		cep = EPServiceProviderManager.getProvider("ArchiveMedianPerHouseBolt", cepConfig);
		cepConfig.addEventType("HistoryBean", HistoryBean.class.getName());
		cepRT = cep.getEPRuntime();
		cepAdm = cep.getEPAdministrator();
		EPStatement cepStatement = cepAdm
				.createEPL("SELECT houseId,timeSlice,average from HistoryBean.std:groupwin(houseId)"
						+ ".win:time_batch(10 sec).stat:weighted_avg(averageLoad,readingsCount) group by houseId");
		cepStatement.setSubscriber(this);

	}

	@Override
	public void execute(Tuple input) {
		cepRT.sendEvent(input.getValue(0));

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
