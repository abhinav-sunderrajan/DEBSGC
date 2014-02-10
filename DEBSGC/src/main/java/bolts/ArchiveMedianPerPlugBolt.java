package bolts;

import java.util.Map;

import main.PlatformCore;

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
import beans.HistoryBean;

public class ArchiveMedianPerPlugBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector _collector;
	private Fields outputFields;

	/**
	 * Initialize with a declaration of the output fields for clarity while
	 * configuring topology.
	 * 
	 * @param outputFields
	 */
	public ArchiveMedianPerPlugBolt(Fields outputFields) {
		this.outputFields = outputFields;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;

	}

	@SuppressWarnings({ "unchecked" })
	@Override
	public void execute(Tuple input) {

		HistoryBean bean = (HistoryBean) input.getValue(0);
		final short houseId = input.getShort(1);
		final short householdId = input.getShort(2);
		final short plugId = input.getShort(3);
		String timeSlice = input.getString(4);

		_collector.emit(new Values(bean, houseId, timeSlice));

		if (!PlatformCore.averageLoadPerPlugPerTimeSlice.get(
				houseId + "_" + householdId + "_" + plugId).containsKey(timeSlice)) {
			Buffer medianList = BufferUtils.synchronizedBuffer(new CircularFifoBuffer(
					PlatformCore.NUMBER_OF_ARCHIVE_STREAMS));
			medianList.add(bean.getAverageLoad());
			PlatformCore.averageLoadPerPlugPerTimeSlice.get(
					houseId + "_" + householdId + "_" + plugId).put(timeSlice, medianList);
		} else {
			Buffer medianList = PlatformCore.averageLoadPerPlugPerTimeSlice.get(
					houseId + "_" + householdId + "_" + plugId).get(timeSlice);
			medianList.add(bean.getAverageLoad());
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
