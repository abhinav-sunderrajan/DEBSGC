package bolts;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class StreamProviderQuery1B extends StreamProviderBolt {

	public StreamProviderQuery1B(String serverIP, int serverPort) throws UnknownHostException {
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
		Long queryLat = (System.currentTimeMillis() - input.getLong(4));
		// long sequence = ringBuffer.next();
		// OutputDF df = ringBuffer.get(sequence);
		// df.clear();
		// df.add(plugId, currentLoad, predictedLoad, time, queryLat);
		if (count == 0) {
			LOGGER.info("HOST NAME\t JVM % FREE\t QUERY LATENCY(ms)\t CURRENT LOAD\t PREDICTED LOAD\t PLUG ID\t TIME");

		}

		count++;

		if (count % 20000 == 0) {
			double totalMem = runtime.totalMemory();
			double freemem = runtime.freeMemory();
			Map<String, Double> map = new HashMap<String, Double>();
			double jvmFreePer = (freemem * 100) / totalMem;
			map.put(localhost, jvmFreePer);
			// df.add(map);
			LOGGER.info(localhost + "\t" + jvmFreePer + "\t" + queryLat + "\t" + currentLoad + "\t"
					+ predictedLoad + "\t" + plugId + "\t" + time);
		}
		// ringBuffer.publish(sequence);

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
