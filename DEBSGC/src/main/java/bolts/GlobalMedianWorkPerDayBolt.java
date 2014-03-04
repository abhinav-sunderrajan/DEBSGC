package bolts;

import java.util.Map;

import org.apache.log4j.Logger;

import utils.ProjectUtils;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import beans.SmartPlugBean;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

public class GlobalMedianWorkPerDayBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private transient EPServiceProvider cep;
	private transient EPAdministrator cepAdm;
	private transient Configuration cepConfig;
	private transient EPRuntime cepRT;
	private OutputCollector _collector;
	private Fields outFields;
	private static long count = 0;
	private static final Logger LOGGER = Logger.getLogger(GlobalMedianWorkPerDayBolt.class);

	public GlobalMedianWorkPerDayBolt(Fields fields) {
		outFields = fields;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		cepConfig = new Configuration();
		cepConfig.getEngineDefaults().getThreading().setListenerDispatchPreserveOrder(false);
		cep = EPServiceProviderManager.getProvider("GlobalMedianWorkPerDayBolt_" + this.hashCode(),
				cepConfig);
		cepConfig.addEventType("SmartPlugBean", SmartPlugBean.class.getName());

		String queries[] = ProjectUtils.getGlobalMedianLoadPerDay();
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
		// TODO Auto-generated method stub

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

}
