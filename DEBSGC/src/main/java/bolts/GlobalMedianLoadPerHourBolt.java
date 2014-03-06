package bolts;

import java.sql.Timestamp;
import java.util.Map;

import org.apache.log4j.Logger;

import utils.ProjectUtils;
import utils.ReservoirMedianEsperAgg;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import beans.SmartPlugBean;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

/**
 * @author abhinav
 * 
 */
public class GlobalMedianLoadPerHourBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Fields outFields;
	private static int count = 0;
	private OutputCollector _collector;
	private transient EPServiceProvider cep;
	private transient EPAdministrator cepAdm;
	private transient Configuration cepConfig;
	private transient EPRuntime cepRT;
	private static final Logger LOGGER = Logger.getLogger(GlobalMedianLoadPerHourBolt.class);

	public GlobalMedianLoadPerHourBolt(Fields fields) {
		outFields = fields;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;

		cepConfig = new Configuration();
		cepConfig.addPlugInAggregationFunction("histMedian",
				ReservoirMedianEsperAgg.class.getName());
		cepConfig.getEngineDefaults().getThreading().setListenerDispatchPreserveOrder(false);
		cep = EPServiceProviderManager.getProvider(
				"GlobalMedianLoadPerHourBolt_" + this.hashCode(), cepConfig);
		cepConfig.addEventType("SmartPlugBean", SmartPlugBean.class.getName());

		String queries[] = ProjectUtils.getGlobalMedianLoadPerHourQuery();
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
		cep.destroy();
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

	public void update(Double median, Long queryEvalTime, SmartPlugBean bean) {
		bean.setGlobalMedian(median);
		bean.setQueryEvalTime(queryEvalTime);
		_collector
				.emit(new Values(bean, bean.getHouseId(), bean.getHouseholdId(), bean.getPlugId()));
		count++;
		if (count % 1000 == 0) {
			LOGGER.info("Global median at " + new Timestamp(bean.getTimestamp()) + " is" + median);
		}

	}
}
