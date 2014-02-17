package bolts;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.BufferUtils;
import org.apache.commons.collections.buffer.CircularFifoBuffer;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import beans.HistoryBean;

public class ArchiveMedianPerPlugBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector _collector;
	private Fields outputFields;
	private Map<String, ConcurrentHashMap<String, Buffer>> averageLoadPerPlugPerTimeSlice;
	private Map stormConf;

	/**
	 * Initialize with a declaration of the output fields for clarity while
	 * configuring topology.
	 * 
	 * @param outputFields
	 */
	public ArchiveMedianPerPlugBolt(Fields outputFields,
			Map<String, ConcurrentHashMap<String, Buffer>> averageLoadPerPlugPerTimeSlice) {
		this.outputFields = outputFields;
		this.averageLoadPerPlugPerTimeSlice = averageLoadPerPlugPerTimeSlice;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		this.stormConf = stormConf;

	}

	@SuppressWarnings({ "unchecked" })
	@Override
	public void execute(Tuple input) {

		HistoryBean bean = (HistoryBean) input.getValue(0);
		if (bean.getHouseholdId() != -1) {
			final short houseId = input.getShort(1);
			final short householdId = input.getShort(2);
			final short plugId = input.getShort(3);
			String timeSlice = input.getString(4);

			_collector.emit(new Values(bean, houseId, timeSlice));

			if (averageLoadPerPlugPerTimeSlice.containsKey(houseId + "_" + householdId + "_"
					+ plugId)) {

				if (!averageLoadPerPlugPerTimeSlice.get(houseId + "_" + householdId + "_" + plugId)
						.containsKey(timeSlice)) {
					Buffer medianList = BufferUtils.synchronizedBuffer(new CircularFifoBuffer(
							(Integer) stormConf.get("NUMBER_OF_DAYS_IN_ARCHIVE")));
					medianList.add(bean.getAverageLoad());
					averageLoadPerPlugPerTimeSlice.get(houseId + "_" + householdId + "_" + plugId)
							.put(timeSlice, medianList);
				} else {
					Buffer medianList = averageLoadPerPlugPerTimeSlice.get(
							houseId + "_" + householdId + "_" + plugId).get(timeSlice);
					medianList.add(bean.getAverageLoad());
				}

			} else {

				ConcurrentHashMap<String, Buffer> bufferMap = new ConcurrentHashMap<String, Buffer>();

				Buffer medianList = BufferUtils.synchronizedBuffer(new CircularFifoBuffer(
						(Integer) stormConf.get("NUMBER_OF_DAYS_IN_ARCHIVE")));
				medianList.add(bean.getAverageLoad());
				bufferMap.put(timeSlice, medianList);
				averageLoadPerPlugPerTimeSlice.put(houseId + "_" + householdId + "_" + plugId,
						bufferMap);

			}
		} else {
			Utils.sleep(1000);
			_collector.emit(new Values(bean, bean.getHouseId(), bean.getTimeSlice()));

		}

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(this.outputFields);

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
