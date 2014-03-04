package utils;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class ProjectUtils {

	private static final int LOAD_PROPERTY = 1;
	private static final int WORK_PROPERTY = 0;

	public static String[] getGlobalMedianLoadPerHourQuery() {
		String query1 = "SELECT MEDIAN(value) as medianVal,current_timestamp,* "
				+ "FROM beans.SmartPlugBean(property=" + LOAD_PROPERTY
				+ ").win:ext_timed(timestamp, 3600 seconds)";
		String[] queries = { query1 };
		return queries;

	}

	public static String[] getGlobalMedianLoadPerDay() {
		String query1 = "SELECT MEDIAN(value) as medianVal,current_timestamp,* "
				+ "FROM beans.SmartPlugBean(property=" + LOAD_PROPERTY
				+ ").win:ext_timed(timestamp, 86400 seconds)";
		String[] queries = { query1 };
		return queries;

	}

	public static String[] getMedianLoadPerPlugPerHour() {
		String query1 = "SELECT MEDIAN(value) as medianLoadPlug,globalMedian"
				+ ",timestamp,(timestamp - 3600 * 1000),queryEvalTime"
				+ ",houseId,householdId,plugId FROM beans.SmartPlugBean(property=" + LOAD_PROPERTY
				+ ").std:groupwin(houseId,householdId,plugId)"
				+ ".win:ext_timed(timestamp, 3600 seconds) GROUP BY houseId,householdId,plugId";
		String[] queries = { query1 };
		return queries;

	}

	public static double round(double value, int places) {
		if (places < 0)
			throw new IllegalArgumentException();

		BigDecimal bd = new BigDecimal(value);
		bd = bd.setScale(places, RoundingMode.HALF_UP);
		return bd.doubleValue();
	}
}
