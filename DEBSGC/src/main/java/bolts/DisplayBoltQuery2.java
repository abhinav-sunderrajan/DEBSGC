package bolts;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import main.PlatformCore;

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

public class DisplayBoltQuery2 implements IRichBolt {

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
	private BarChartDisplay barchart;
	public final String PERCENTAGE_PLUGS = "PERCENTAGE_PLUGS";
	private HashMap<String, Double> barchartValuesMap;

	@SuppressWarnings("deprecation")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		display = StreamJoinDisplay.getInstance("Join Performance Measure",
				PlatformCore.configProperties.getProperty("image.save.directory"));

		barchart = new BarChartDisplay("Outlier Detection", "House ID",
				"% Plugs above global median",
				PlatformCore.configProperties.getProperty("image.save.directory"),
				new DefaultCategoryDataset());
		barchartValuesMap = new HashMap<String, Double>();
		barchartValuesMap.put(PERCENTAGE_PLUGS, 0.0);

		for (short i = 0; i < 40; i++) {
			for (Entry<String, Double> entry : barchartValuesMap.entrySet()) {
				barchart.getDataset().addValue(entry.getValue(), entry.getKey(), String.valueOf(i));
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

		Long queryLat = input.getLong(3);
		Double percentage = input.getDouble(2);
		Integer houseId = input.getInteger(1);
		String time = input.getString(0);
		if (queryLat == null || percentage == null || time == null) {
			barchart.getChart().setTitle("Outlier Detection " + time);
		} else {

			if (throughputFlag) {
				timer.set(Calendar.getInstance().getTimeInMillis());
				numOfMsgsin30Sec = count;
			}
			count++;
			throughputFlag = false;
			barchartValuesMap.put(PERCENTAGE_PLUGS, round(percentage, 2));
			barchart.refreshDisplayValues(houseId, barchartValuesMap, time);
			// Refresh display values every 30 seconds
			if ((Calendar.getInstance().getTimeInMillis() - timer.get()) >= 30000) {
				double throughput = (1000 * (count - numOfMsgsin30Sec))
						/ (Calendar.getInstance().getTimeInMillis() - timer.get());
				latency = queryLat;
				valueMap.put((1 + this.hashCode()), latency / 1.0);
				valueMap.put((2 + this.hashCode()), throughput);
				display.refreshDisplayValues(valueMap);
				throughputFlag = true;
			}
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

	public static double round(double value, int places) {
		if (places < 0)
			throw new IllegalArgumentException();

		BigDecimal bd = new BigDecimal(value);
		bd = bd.setScale(places, RoundingMode.HALF_UP);
		return bd.doubleValue();
	}

}
