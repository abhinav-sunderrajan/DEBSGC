package bolts;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jfree.chart.JFreeChart;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * This bolt is responsible for sending the tuples to the {@link JFreeChart}s
 * 
 * @author abhinav
 * 
 */
public class StreamProviderQuery1A extends StreamProviderBolt {

	public StreamProviderQuery1A(String serverIP, int serverPort) {
		super(serverIP, serverPort);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private long count = 0;
	private static final Logger LOGGER = Logger.getLogger(StreamProviderQuery1A.class);

	@Override
	public void execute(Tuple input) {
		Short houseId = input.getShort(0);
		Double currentLoad = input.getDouble(1);
		Double predictedLoad = input.getDouble(2);
		String time = input.getString(3);
		Long queryLat = System.currentTimeMillis() - input.getLong(4);
		// long sequence = ringBuffer.next();
		// OutputDF df = ringBuffer.get(sequence);
		// df.clear();
		// df.add(houseId, currentLoad, predictedLoad, time, queryLat);
		if (count == 0) {
			LOGGER.info("HOST NAME\t JVM % FREE(ms)\t QUERY LATENCY\t CURRENT LOAD\t PREDICTED LOAD\t HOUSE ID\t TIME");

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
					+ predictedLoad + "\t" + houseId + "\t" + time);

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
