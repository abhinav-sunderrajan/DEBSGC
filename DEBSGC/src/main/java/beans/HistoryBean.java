package beans;

import java.io.Serializable;

/**
 * The bean representing the historical load aggregates between the time period
 * specified.
 * 
 * @author abhinav
 * 
 */
public class HistoryBean implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private short houseId;
	private short householdId;
	private short plugId;
	private float averageLoad;
	private int readingsCount;
	private String timeSlice;

	public HistoryBean() {
	}

	public HistoryBean(short houseId, short householdId, short plugId, float averageLoad,
			int readingsCount, String timeSlice) {
		this.houseId = houseId;
		this.householdId = householdId;
		this.plugId = plugId;
		this.averageLoad = averageLoad;
		this.readingsCount = readingsCount;
		this.timeSlice = timeSlice;
	}

	public short getHouseId() {
		return houseId;
	}

	public void setHouseId(short houseId) {
		this.houseId = houseId;
	}

	public short getHouseholdId() {
		return householdId;
	}

	public void setHouseholdId(short householdId) {
		this.householdId = householdId;
	}

	public short getPlugId() {
		return plugId;
	}

	public void setPlugId(short plugId) {
		this.plugId = plugId;
	}

	public float getAverageLoad() {
		return averageLoad;
	}

	public void setAverageLoad(float averageLoad) {
		this.averageLoad = averageLoad;
	}

	public int getReadingsCount() {
		return readingsCount;
	}

	public void setReadingsCount(int readingsCount) {
		this.readingsCount = readingsCount;
	}

	public String getTimeSlice() {
		return timeSlice;
	}

	public void setTimeSlice(String timeSlice) {
		this.timeSlice = timeSlice;
	}

}
