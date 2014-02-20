package utils;

import java.util.ArrayList;

public class CircularList<E> extends ArrayList<E> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int bufferSize;

	public CircularList(int size) {
		this.bufferSize = size;
	}

	public CircularList() {
		this.bufferSize = 30;
	}

	@Override
	public boolean add(E obj) {
		if (super.size() == bufferSize) {
			for (int i = 1; i < bufferSize; i++) {
				super.set(i - 1, super.get(i));
			}
			super.remove(bufferSize - 1);
		}
		super.add(obj);
		return true;
	}
}
