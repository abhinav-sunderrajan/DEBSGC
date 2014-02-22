package bolts;

import java.util.Calendar;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jfree.chart.JFreeChart;

import utils.OutputDF;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * This bolt is responsible for sending the tuples to the {@link JFreeChart}s
 * 
 * @author abhinav
 * 
 */
public class StreamProviderQuery1A extends StreamProviderBolt<OutputDF> {

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
		Long queryLat = Calendar.getInstance().getTimeInMillis() - input.getLong(4);
		buffer.add(new OutputDF(houseId, currentLoad, predictedLoad, time, queryLat));
		count++;
		if (count % 5000 == 0) {
			LOGGER.info("Current load:" + currentLoad + " Predicted load: " + predictedLoad
					+ " at " + time);
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
