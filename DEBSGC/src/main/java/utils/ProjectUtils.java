package utils;

import java.math.BigDecimal;
import java.math.RoundingMode;

import main.PlatformCore;

public class ProjectUtils {

	public static String[] getGlobalMedianLoadPerHour() {
		String query1 = "SELECT MEDIAN(value) as medianVal,current_timestamp,* FROM beans.SmartPlugBean(property="
				+ PlatformCore.LOAD_PROPERTY + ").win:ext_timed(timestamp, 3600 seconds)";
		String[] queries = { query1 };
		return queries;

	}

	public static String[] getMedianLoadPerPlugPerHour() {
		String query1 = "SELECT MEDIAN(value) as medianLoadPlug,globalMedian"
				+ ",timestamp,(timestamp - 3600 * 1000),queryEvalTime"
				+ ",houseId,householdId,plugId FROM beans.SmartPlugBean(property="
				+ PlatformCore.LOAD_PROPERTY + ").std:groupwin(houseId,householdId,plugId)"
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
