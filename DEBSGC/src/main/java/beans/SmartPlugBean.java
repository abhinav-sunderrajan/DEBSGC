package beans;

import java.io.Serializable;

public class SmartPlugBean implements Serializable {

	/**
    * 
    */
	private static final long serialVersionUID = 1L;
	private long id;
	private long timestamp;
	private float value;
	private short property;
	private short plugId;
	private short householdId;
	private short houseId;
	private double globalMedian;
	private long queryEvalTime;

	/**
	 * @param id
	 * @param timestamp
	 * @param value
	 * @param property
	 * @param plugId
	 * @param householdId
	 * @param houseId
	 */
	public SmartPlugBean(long id, long timestamp, float value, short property, short plugId,
			short householdId, short houseId) {
		super();
		this.id = id;
		this.timestamp = timestamp;
		this.value = value;
		this.property = property;
		this.plugId = plugId;
		this.householdId = householdId;
		this.houseId = houseId;
	}

	public SmartPlugBean() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @return the id
	 */
	public long getId() {
		return id;
	}

	/**
	 * @param id
	 *            the id to set
	 */
	public void setId(long id) {
		this.id = id;
	}

	/**
	 * @return the timestamp
	 */
	public long getTimestamp() {
		return timestamp;
	}

	/**
	 * @param timestamp
	 *            the timestamp to set
	 */
	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * @return the value
	 */
	public float getValue() {
		return value;
	}

	/**
	 * @param value
	 *            the value to set
	 */
	public void setValue(float value) {
		this.value = value;
	}

	/**
	 * @return the property
	 */
	public short getProperty() {
		return property;
	}

	/**
	 * @param property
	 *            the property to set
	 */
	public void setProperty(short property) {
		this.property = property;
	}

	/**
	 * @return the plugId
	 */
	public short getPlugId() {
		return plugId;
	}

	/**
	 * @param plugId
	 *            the plugId to set
	 */
	public void setPlugId(short plugId) {
		this.plugId = plugId;
	}

	/**
	 * @return the householdId
	 */
	public short getHouseholdId() {
		return householdId;
	}

	/**
	 * @param householdId
	 *            the householdId to set
	 */
	public void setHouseholdId(short householdId) {
		this.householdId = householdId;
	}

	/**
	 * @return the houseId
	 */
	public short getHouseId() {
		return houseId;
	}

	/**
	 * @param houseId
	 *            the houseId to set
	 */
	public void setHouseId(short houseId) {
		this.houseId = houseId;
	}

	public double getGlobalMedian() {
		return globalMedian;
	}

	public void setGlobalMedian(double globalMedian) {
		this.globalMedian = globalMedian;
	}

	public long getQueryEvalTime() {
		return queryEvalTime;
	}

	public void setQueryEvalTime(long queryEvalTime) {
		this.queryEvalTime = queryEvalTime;
	}

}
