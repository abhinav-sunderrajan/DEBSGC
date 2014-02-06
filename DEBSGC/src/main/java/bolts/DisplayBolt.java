package bolts;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import main.PlatformCore;

import org.jfree.chart.JFreeChart;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.time.Minute;
import org.jfree.data.time.TimeSeries;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import display.BarChartDisplay;
import display.StreamJoinDisplay;

/**
 * This bolt is responsible for sending the tuples to the {@link JFreeChart}s
 * 
 * @author abhinav
 * 
 */
public class DisplayBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private long count = 0;
	private long numOfMsgsin30Sec = 0;
	private StreamJoinDisplay display;
	private long latency;
	private Map<Integer, Double> valueMap;
	private AtomicLong timer;
	private boolean throughputFlag;
	private BarChartDisplay valuesOutput;

	@SuppressWarnings("deprecation")
	public DisplayBolt(int streamRate) {
		display = StreamJoinDisplay.getInstance("Join Performance Measure",
				PlatformCore.configProperties.getProperty("image.save.directory"));

		valuesOutput = new BarChartDisplay("Load Prediction",
				PlatformCore.configProperties.getProperty("image.save.directory"),
				new DefaultCategoryDataset());

		timer = new AtomicLong(0);
		throughputFlag = true;
		display.addToDataSeries(new TimeSeries("Latency for Subscriber#" + this.hashCode()
				+ " in msec", Minute.class), (1 + this.hashCode()));
		display.addToDataSeries(new TimeSeries("Throughput/sec for Subscriber# " + this.hashCode(),
				Minute.class), (2 + this.hashCode()));
		valueMap = new HashMap<Integer, Double>();
		valueMap.put((2 + this.hashCode()), 0.0);
		valueMap.put((1 + this.hashCode()), 0.0);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

	}

	@Override
	public void execute(Tuple input) {

		if (throughputFlag) {
			timer.set(Calendar.getInstance().getTimeInMillis());
			numOfMsgsin30Sec = count;
		}
		count++;
		throughputFlag = false;
		valuesOutput.refreshDisplayValues(input.getShort(0), input.getDouble(1),
				input.getDouble(2), input.getString(3));
		// Refresh display values every 30 seconds
		if ((Calendar.getInstance().getTimeInMillis() - timer.get()) >= 30000) {
			double throughput = (1000 * (count - numOfMsgsin30Sec))
					/ (Calendar.getInstance().getTimeInMillis() - timer.get());
			latency = Calendar.getInstance().getTimeInMillis() - input.getLong(4);
			valueMap.put((1 + this.hashCode()), latency / 1.0);
			valueMap.put((2 + this.hashCode()), throughput);
			display.refreshDisplayValues(valueMap);
			throughputFlag = true;
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
