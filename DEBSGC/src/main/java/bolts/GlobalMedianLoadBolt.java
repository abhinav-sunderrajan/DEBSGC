package bolts;

import java.sql.Timestamp;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import beans.SmartPlugBean;

/**
 * @author abhinav
 * 
 */
public class GlobalMedianLoadBolt extends EsperEnrichedBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Fields outputFields;
	private static final Logger LOGGER = Logger.getLogger(GlobalMedianLoadBolt.class);
	private static int count = 0;

	/**
	 * 
	 * @param eventTypes
	 *            - The event type beans expected by the Esper engine instance.
	 * @param queries
	 *            - The queries registered with this instance.
	 * @param esperEngineName
	 *            - The name of the Esper engine instance.
	 * @param outputFields
	 *            - output fields of this bolt.
	 */
	public GlobalMedianLoadBolt(String[] eventTypes, String[] queries, String esperEngineName,
			Fields outputFields) {
		super(eventTypes, queries, esperEngineName);
		this.outputFields = outputFields;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(outputFields);
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	public void update(Double median, Long queryEvalTime, SmartPlugBean bean) {
		if (count % 1000 == 0) {
			LOGGER.info("Global median is " + median + " at:" + new Timestamp(bean.getTimestamp()));
		}

		bean.setGlobalMedian(median);
		bean.setQueryEvalTime(queryEvalTime);
		_collector
				.emit(new Values(bean, bean.getHouseId(), bean.getHouseholdId(), bean.getPlugId()));
		count++;

	}
}
