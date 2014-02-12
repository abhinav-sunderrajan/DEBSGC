package beans;

import java.io.Serializable;

/**
 * Calculates the current load average per house of the live stream by using
 * sliding window.
 * 
 * @author abhinav
 * 
 */
public class CurrentLoadPerHouseBean implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private short houseId;
	private double currentAverageLoad;
	private Long currTime;
	private Long evaluationTime;

	/**
	 * 
	 * @param houseId
	 * @param currentAverageLoad
	 * @param currTime
	 * @param evaluationTime
	 */
	public CurrentLoadPerHouseBean(short houseId, double currentAverageLoad, Long currTime,
			Long evaluationTime) {
		super();
		this.houseId = houseId;
		this.currentAverageLoad = currentAverageLoad;
		this.currTime = currTime;
		this.setEvaluationTime(evaluationTime);
	}

	public short getHouseId() {
		return houseId;
	}

	public void setHouseId(short houseId) {
		this.houseId = houseId;
	}

	public double getCurrentAverageLoad() {
		return currentAverageLoad;
	}

	public void setCurrentAverageLoad(double currentAverageLoad) {
		this.currentAverageLoad = currentAverageLoad;
	}

	public Long getCurrTime() {
		return currTime;
	}

	public void setCurrTime(Long currTime) {
		this.currTime = currTime;
	}

	public Long getEvaluationTime() {
		return evaluationTime;
	}

	public void setEvaluationTime(Long evaluationTime) {
		this.evaluationTime = evaluationTime;
	}

}
