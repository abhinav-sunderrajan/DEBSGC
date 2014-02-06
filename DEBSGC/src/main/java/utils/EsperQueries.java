package utils;

public class EsperQueries {

	public static String[] getGlobalMedianLoad(long timeIntervalInMillSec) {
		String s1 = "CREATE VARIABLE long start = 0";
		String s2 = "ON beans.SmartPlugBean(timestamp > (start+" + timeIntervalInMillSec
				+ ")) SET start=(start +" + timeIntervalInMillSec + ")";
		String s3 = "select MEDIAN(value) as medianVal,timestamp FROM beans.SmartPlugBean"
				+ ".win:keepall().win:expr(timestamp < start+" + timeIntervalInMillSec + ") ";
		String[] queries = { s1, s2, s3 };

		return queries;

	}

	public static String[] getMedianLoadPerPlug(long timeIntervalInMillSec) {
		String s1 = "CREATE VARIABLE long start = 0";
		String s2 = "ON beans.SmartPlugBean(timestamp > (start+" + timeIntervalInMillSec
				+ ")) SET start=(start +" + timeIntervalInMillSec + ")";

		String s3 = "@Hint('reclaim_group_aged="
				+ (timeIntervalInMillSec / 1000)
				+ "') "
				+ "SELECT MEDIAN(load),timestamp,plugid,houseid FROM SmartPlugBean.std:groupwin(plugid)"
				+ ".win:keepall().win:expr(timestamp < start+" + timeIntervalInMillSec + ") ";
		String[] queries = { s1, s2, s3 };

		return queries;

	}
}
