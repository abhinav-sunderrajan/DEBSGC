package utils;

import java.util.Map.Entry;
import java.util.TreeMap;

import com.espertech.esper.epl.agg.service.AggregationSupport;
import com.espertech.esper.epl.agg.service.AggregationValidationContext;

/**
 * Custom aggregation function which computes the median from histogram to
 * reduce the memory requirements.
 * 
 * @author abhinav
 * 
 */
public class MedianFromHistogram extends AggregationSupport {
	private TreeMap<Float, Integer> sortedMap = new TreeMap<Float, Integer>();
	private int count = 0;

	@Override
	public void enter(Object value) {

		if (value == null) {
			return;
		}

		count++;
		if (value instanceof Float) {
			Float k = (Float) value;

			if (sortedMap.containsKey(k)) {
				int val = sortedMap.get(k);
				sortedMap.put(k, ++val);
			} else {
				sortedMap.put(k, 1);
			}

		}

	}

	@Override
	public void leave(Object value) {
		if (value == null) {
			return;
		}

		--count;
		if (value instanceof Float) {
			Float k = (Float) value;

			if (sortedMap.containsKey(k)) {
				int val = sortedMap.get(k);
				sortedMap.put(k, --val);
			}

		}

	}

	@Override
	public Object getValue() {
		if (count == 0) {
			return null;
		}

		return calculateMedian();
	}

	@Override
	public Class getValueType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void clear() {
		sortedMap.clear();

	}

	@Override
	public void validate(AggregationValidationContext validationContext) {
		// for (Class clazz : validationContext.getParameterTypes()) {
		// if (!clazz.isAssignableFrom(Number.class)) {
		// throw new RuntimeException("Argument must a Number");
		// }
		// }

	}

	private Double calculateMedian() {
		Double median = 0.0;
		int len = (count + 1) / 2;

		int current = 0;
		for (Entry<Float, Integer> entry : sortedMap.entrySet()) {
			current = +entry.getValue();
			if (current >= len) {
				median = entry.getKey().doubleValue();
			}
		}

		return median;

	}

}
