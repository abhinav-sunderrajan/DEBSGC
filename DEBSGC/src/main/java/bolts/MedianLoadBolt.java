package bolts;

import java.sql.Timestamp;
import java.util.Map;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author abhinav
 * 
 */
public class MedianLoadBolt extends EsperEnrichedBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public MedianLoadBolt(String[] eventTypes, String[] queries, String esperEngineName) {
		super(eventTypes, queries, esperEngineName);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("median", "timestamp"));
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

	public void update(Double median, Timestamp timestamp) {
		_collector.emit(new Values(median, timestamp));

	}

}
