package bolts;

import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
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
public class DisplayBoltQuery1A implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private long count = 0;
	private long numOfMsgsin30Sec = 0;
	private transient StreamJoinDisplay display;
	private long latency;
	private Map<Integer, Double> valueMap;
	private AtomicLong timer;
	private boolean throughputFlag;
	private transient BarChartDisplay valuesOutput;
	public final String CURRENT_LOAD = "Current Load";
	public final String PREDICTED_LOAD = "Predicted Load";
	private HashMap<String, Double> barchartDisplayMap;

	@SuppressWarnings("deprecation")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

		display = StreamJoinDisplay.getInstance("Join Performance Measure",
				PlatformCore.configProperties.getProperty("image.save.directory"));

		valuesOutput = new BarChartDisplay("Load Prediction", "House ID", "Load(W)",
				PlatformCore.configProperties.getProperty("image.save.directory"),
				new DefaultCategoryDataset());
		barchartDisplayMap = new LinkedHashMap<String, Double>();
		barchartDisplayMap.put(CURRENT_LOAD, 0.0);
		barchartDisplayMap.put(PREDICTED_LOAD, 0.0);

		for (short i = 0; i < 40; i++) {
			for (Entry<String, Double> entry : barchartDisplayMap.entrySet()) {
				valuesOutput.getDataset().addValue(entry.getValue(), entry.getKey(),
						String.valueOf(i));
			}

		}

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
	public void execute(Tuple input) {

		if (throughputFlag) {
			timer.set(Calendar.getInstance().getTimeInMillis());
			numOfMsgsin30Sec = count;
		}
		count++;
		throughputFlag = false;
		barchartDisplayMap.put(CURRENT_LOAD, input.getDouble(1));
		barchartDisplayMap.put(PREDICTED_LOAD, input.getDouble(2));
		valuesOutput
				.refreshDisplayValues(input.getShort(0), barchartDisplayMap, input.getString(3));
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
