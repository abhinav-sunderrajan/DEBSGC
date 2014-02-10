package utils;

import main.PlatformCore;

public class EsperQueries {

	public static String[] getGlobalMedianLoadPerHour() {
		String query1 = "SELECT MEDIAN(value) as medianVal,current_timestamp,* FROM beans.SmartPlugBean(property="
				+ PlatformCore.LOAD_PROPERTY + ").win:ext_timed(timestamp, 3600 seconds)";
		String[] queries = { query1 };
		return queries;

	}

	public static String[] getMedianLoadPerPlugPerHour() {
		String query1 = "SELECT MEDIAN(load) as medianLoadPlug,globalMedian"
				+ ",timestamp,(timestamp - 3600 * 1000),queryEvalTime"
				+ ",houseid,householdId,plugid FROM SmartPlugBean(property="
				+ PlatformCore.LOAD_PROPERTY
				+ ").std:groupwin(houseId,householdId,plugId).win:ext_timed(timestamp, 3600 seconds)";
		String[] queries = { query1 };
		return queries;

	}
}
