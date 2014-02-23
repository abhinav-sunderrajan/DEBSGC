package bolts;

import java.util.Map;

import org.apache.log4j.Logger;

import utils.OutputDF;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class StreamProviderQuery1B extends StreamProviderBolt {

	public StreamProviderQuery1B(String serverIP, int serverPort) {
		super(serverIP, serverPort);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private long count = 0;
	private static final Logger LOGGER = Logger.getLogger(StreamProviderQuery1B.class);

	@Override
	public void execute(Tuple input) {
		String plugId = input.getString(0);
		Double currentLoad = input.getDouble(1);
		Double predictedLoad = input.getDouble(2);
		String time = input.getString(3);
		Long queryLat = System.currentTimeMillis() - input.getLong(4);
		if (queryLat < 0) {
			queryLat = 2l;
		}
		long sequence = ringBuffer.next();
		OutputDF df = ringBuffer.get(sequence);
		df.clear();
		df.add(plugId, currentLoad, predictedLoad, time, queryLat);
		ringBuffer.publish(sequence);
		count++;
		if (count % 2000 == 0) {
			LOGGER.info("Current load:" + currentLoad + " Predicted load:" + predictedLoad
					+ " for:" + plugId + " at:" + time);
			LOGGER.info("Query latency is milli secs is " + queryLat);
		}

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
