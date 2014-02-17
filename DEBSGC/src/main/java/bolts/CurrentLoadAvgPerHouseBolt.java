package bolts;

import java.text.ParseException;
import java.util.Map;

import main.PlatformCore;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import beans.CurrentLoadPerHouseBean;
import beans.SmartPlugBean;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

public class CurrentLoadAvgPerHouseBolt implements IRichBolt {

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
	private static final Logger LOGGER = Logger.getLogger(CurrentLoadAvgPerHouseBolt.class);

	/**
	 * Initialize with the size of the window for calculating the current
	 * average and output Fields
	 * 
	 * @param avgCalcInterval
	 * @param outFields
	 * @throws ParseException
	 */
	public CurrentLoadAvgPerHouseBolt(long avgCalcInterval, Fields outFields) throws ParseException {
		this.avgCalcInterval = avgCalcInterval;
		this.outFields = outFields;
	}

	public void update(Integer houseId, Double averageLoad, Long timestamp, Long evaluationTime) {
		_collector.emit(new Values(houseId, new CurrentLoadPerHouseBean(houseId.shortValue(),
				averageLoad, timestamp, evaluationTime)));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		cepConfig = new Configuration();
		cepConfig.getEngineDefaults().getThreading().setListenerDispatchPreserveOrder(false);
		cep = EPServiceProviderManager.getProvider("CurrentLoadAvgPerHouseBolt_" + this.hashCode(),
				cepConfig);
		cepConfig.addEventType("SmartPlugBean", SmartPlugBean.class.getName());
		cepRT = cep.getEPRuntime();
		cepAdm = cep.getEPAdministrator();
		cepAdm.createEPL("create variable long LL = " + stormConf.get("liveStartTime"));
		cepAdm.createEPL("create variable long UL = "
				+ ((Long) stormConf.get("liveStartTime") + avgCalcInterval - 1000));
		cepAdm.createEPL("on beans.SmartPlugBean(timestamp > UL) set LL=(LL+" + avgCalcInterval
				+ "), UL=(UL+" + avgCalcInterval + ") ");
		EPStatement cepStatement = cepAdm
				.createEPL("select houseId,AVG(value) as avgVal,timestamp,current_timestamp FROM "
						+ "beans.SmartPlugBean(property=" + PlatformCore.LOAD_PROPERTY
						+ ").std:groupwin(houseId).win:keepall()"
						+ ".win:expr(timestamp >=LL AND timestamp<UL) group by houseId");
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
