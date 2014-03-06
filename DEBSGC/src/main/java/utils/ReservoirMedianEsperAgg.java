package utils;

import java.util.Random;

import org.apache.commons.collections.list.TreeList;

import com.espertech.esper.epl.agg.service.AggregationSupport;
import com.espertech.esper.epl.agg.service.AggregationValidationContext;

/**
 * Implements reservoir sampling over a stream of incoming data. Especially
 * useful when the window size is too large for any sort of aggregation to be
 * performed.
 * 
 * @author abhinav
 * 
 */
public class ReservoirMedianEsperAgg extends AggregationSupport {

	private TreeList sortedVector = new TreeList();
	private int count = 0;
	private Random random = new Random();
	private final int reservoirSize = 1000;

	@SuppressWarnings("unchecked")
	@Override
	public void enter(Object value) {

		if (value == null) {
			return;
		}

		count++;
		if (value instanceof Number) {
			if (sortedVector.size() < 2) {
				sortedVector.add(value);

			} else {

				if (random.nextDouble() < ((double) sortedVector.size()) / reservoirSize) {
					int replace = random.nextInt(sortedVector.size());
					sortedVector.remove(replace);
					sortedVector.add(value);

				} else {
					sortedVector.add(value);
				}

			}

		}

	}

	@Override
	public void leave(Object value) {
		if (value == null) {
			return;
		}

		--count;
		if (value instanceof Number) {
			if (sortedVector.contains(value))
				sortedVector.remove(value);

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
		sortedVector.clear();

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
		Float globalMedian = 0.0f;
		if (sortedVector.size() == 1) {
			globalMedian = (Float) sortedVector.get(0);
		} else if (sortedVector.size() % 2 == 0) {
			globalMedian = (Float) sortedVector.get(sortedVector.size() / 2)
					+ (Float) sortedVector.get((sortedVector.size() / 2) - 1);
		} else {
			globalMedian = (Float) sortedVector.get((sortedVector.size() / 2) + 1);
		}

		return globalMedian.doubleValue();

	}

}
