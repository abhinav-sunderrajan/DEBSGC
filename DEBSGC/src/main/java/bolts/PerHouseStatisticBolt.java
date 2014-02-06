package bolts;

import java.sql.Timestamp;
import java.util.Map;

import main.PlatformCore;
import utils.EsperQueries;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import beans.SmartPlugBean;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

public class PerHouseStatisticBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private EPServiceProvider cep;
	private EPAdministrator cepAdm;
	private Configuration cepConfig;
	private EPRuntime cepRT;
	private OutputCollector _collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		cepConfig = new Configuration();
		cepConfig.getEngineDefaults().getThreading().setListenerDispatchPreserveOrder(false);
		cep = EPServiceProviderManager.getProvider("PER_HOUSE_STATISTIC", cepConfig);
		cepConfig.addEventType("SmartPlugBean", SmartPlugBean.class.getName());

		String queries[] = EsperQueries.getMedianLoadPerPlug(Long
				.parseLong(PlatformCore.configProperties.getProperty("query2.window.size")));
		EPStatement cepStatement = null;
		cepRT = cep.getEPRuntime();
		cepAdm = cep.getEPAdministrator();
		for (int i = 0; i < queries.length; i++) {
			if (i == queries.length - 1) {
				cepStatement = cepAdm.createEPL(queries[i]);
			} else {
				cepAdm.createEPL(queries[i]);
			}

		}

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
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void update(Double medianLoad, Timestamp timestamp, Integer plugId, Integer houseId) {
		_collector.emit(new Values());

	}

}
