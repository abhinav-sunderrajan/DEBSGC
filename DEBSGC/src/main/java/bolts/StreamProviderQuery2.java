package bolts;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import utils.OutputDF;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class StreamProviderQuery2 extends StreamProviderBolt {
	private long count = 0;
	private static final Logger LOGGER = Logger.getLogger(StreamProviderQuery2.class);

	public StreamProviderQuery2(String serverIP, int serverPort) {
		super(serverIP, serverPort);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void execute(Tuple input) {
		Long queryLat = input.getLong(3);
		Double percentage = input.getDouble(2);
		Integer houseId = input.getInteger(1);
		String time = input.getString(0);
		long sequence = ringBuffer.next();
		OutputDF df = ringBuffer.get(sequence);
		df.clear();
		df.add(queryLat, percentage, houseId, time);
		count++;
		if (count % 1000 == 0) {
			LOGGER.info("% of plugs above global median for houseId " + houseId + " at " + time
					+ " is " + percentage);
			LOGGER.info("Query latency is milli secs is " + queryLat);
			double totalMem = runtime.totalMemory();
			double freemem = runtime.freeMemory();
			Map<String, Double> map = new HashMap<String, Double>();
			map.put(localhost, (freemem * 100) / totalMem);
			LOGGER.info("free mem at " + localhost + " is " + freemem);
			df.add(map);
		}
		ringBuffer.publish(sequence);

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
		return null;
	}

}
