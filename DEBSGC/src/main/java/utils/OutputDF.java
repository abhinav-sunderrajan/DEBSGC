package utils;

import java.util.ArrayList;

import com.lmax.disruptor.EventFactory;

/**
 * A convenience class for making tuple values using new Values("field1", 2, 3)
 * syntax.
 */
public class OutputDF extends ArrayList<Object> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public final static EventFactory<OutputDF> EVENT_FACTORY = new EventFactory<OutputDF>() {
		public OutputDF newInstance() {
			return new OutputDF();
		}
	};

	public OutputDF() {

	}

	public OutputDF(Object... vals) {
		super(vals.length);
		for (Object o : vals) {
			add(o);
		}
	}

	public void add(Object... vals) {
		for (Object o : vals) {
			add(o);
		}
	}
}