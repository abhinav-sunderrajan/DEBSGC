package bolts;

import java.text.ParseException;
import java.util.Map;

import main.PlatformCore;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import beans.CurrentLoadPerPlugBean;
import beans.SmartPlugBean;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

/**
 * Calculates the current load average per plug of the live stream by using
 * sliding windows.
 * 
 * @author abhinav
 * 
 */
public class CurrentLoadAvgPerPlugBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private transient EPServiceProvider cep;
	private transient EPAdministrator cepAdm;
	private transient Configuration cepConfig;
	private transient EPRuntime cepRT;
	private OutputCollector _collector;
	private long avgCalcInterval;
	private Fields outFields;

	/**
	 * Initialize with the size of the window for calculating the current
	 * average and output Fields
	 * 
	 * @param avgCalcInterval
	 * @param outFields
	 * @throws ParseException
	 */
	public CurrentLoadAvgPerPlugBolt(long avgCalcInterval, Fields outFields) throws ParseException {
		this.avgCalcInterval = avgCalcInterval;
		this.outFields = outFields;
	}

	public void update(Integer houseId, Integer householdId, Integer plugId, Double averageLoad,
			Long timestamp, Long evaluationTime) {
		_collector.emit(new Values(houseId, householdId, plugId, new CurrentLoadPerPlugBean(houseId
				.shortValue(), householdId.shortValue(), plugId.shortValue(), averageLoad,
				timestamp, evaluationTime)));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		cepConfig = new Configuration();
		cepConfig.getEngineDefaults().getThreading().setListenerDispatchPreserveOrder(false);
		cep = EPServiceProviderManager.getProvider("CurrentLoadAvgPerPlugBolt_" + this.hashCode(),
				cepConfig);
		cepConfig.addEventType("SmartPlugBean", SmartPlugBean.class.getName());
		cepRT = cep.getEPRuntime();
		cepAdm = cep.getEPAdministrator();
		cepAdm.createEPL("create variable long LL = "
				+ Long.parseLong((String) stormConf.get("live.start.time")));
		cepAdm.createEPL("create variable long UL = "
				+ ((Long.parseLong((String) stormConf.get("live.start.time")) + avgCalcInterval - 1000)));
		cepAdm.createEPL("on beans.SmartPlugBean(timestamp > UL) set LL=(LL+" + avgCalcInterval
				+ "), UL=(UL+" + avgCalcInterval + ") ");
		EPStatement cepStatement = cepAdm
				.createEPL("select houseId,householdId,plugId, AVG(value) as "
						+ "avgVal,timestamp,current_timestamp FROM "
						+ "beans.SmartPlugBean(property="
						+ PlatformCore.LOAD_PROPERTY
						+ ").std:groupwin(houseId,householdId,plugId).win:keepall()"
						+ ".win:expr(timestamp >=LL AND timestamp<UL) group by houseId,householdId,plugId");
		cepStatement.setSubscriber(this);

	}

	@Override
	public void execute(Tuple input) {
		cepRT.sendEvent((SmartPlugBean) input.getValue(0));

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
