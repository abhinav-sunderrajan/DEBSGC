package bolts;

import java.sql.Timestamp;
import java.util.Map;

import org.apache.log4j.Logger;

import utils.ProjectUtils;
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

public class MedianLoadPerPlugPerHourBolt implements IRichBolt {

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
	private static final Logger LOGGER = Logger.getLogger(MedianLoadPerPlugPerHourBolt.class);

	public MedianLoadPerPlugPerHourBolt(Fields fields) {
		outFields = fields;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		cepConfig = new Configuration();
		cepConfig.getEngineDefaults().getThreading().setListenerDispatchPreserveOrder(false);
		cep = EPServiceProviderManager.getProvider("PerPlugMedianBolt_" + this.hashCode(),
				cepConfig);
		cepConfig.addEventType("SmartPlugBean", SmartPlugBean.class.getName());

		String queries[] = ProjectUtils.getMedianLoadPerPlugPerHour();
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

	public void update(Double medianLoad, Double globalMedian, Long timestampStart,
			Long queryEvalTime, Integer houseId, Integer householdId, Integer plugId) {
		if (count % 1000 == 0) {
			LOGGER.info("median for plug " + houseId + "_" + "_" + householdId + "_" + plugId
					+ " is " + medianLoad + " at " + new Timestamp(timestampStart));
		}

		Long timestampEnd = timestampStart - 3600 * 1000;
		_collector.emit(new Values(medianLoad, globalMedian, timestampStart, timestampEnd,
				queryEvalTime, houseId, householdId, plugId));
		count++;

	}
}
