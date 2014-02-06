package beans;

import java.io.Serializable;

public class CurrentLoadPerPlugBean implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private short houseId;
	private short houseHoldId;
	private short plugId;
	private double currentAverageLoad;
	private Long currTime;
	private Long evaluationTime;

	public CurrentLoadPerPlugBean(short houseId, short houseHoldId, short plugId,
			double currentAverageLoad, Long currTime, Long evaluationTime) {
		this.houseId = houseId;
		this.houseHoldId = houseHoldId;
		this.plugId = plugId;
		this.currentAverageLoad = currentAverageLoad;
		this.currTime = currTime;
		this.evaluationTime = evaluationTime;
	}

	public short getHouseId() {
		return houseId;
	}

	public void setHouseId(short houseId) {
		this.houseId = houseId;
	}

	public short getHouseHoldId() {
		return houseHoldId;
	}

	public void setHouseHoldId(short houseHoldId) {
		this.houseHoldId = houseHoldId;
	}

	public short getPlugId() {
		return plugId;
	}

	public void setPlugId(short plugId) {
		this.plugId = plugId;
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
