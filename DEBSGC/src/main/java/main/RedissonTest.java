package main;

import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.redisson.Config;
import org.redisson.Redisson;

import utils.CircularList;

public class RedissonTest {

	private Map<Integer, ConcurrentHashMap<String, CircularList<Double>>> averageLoadPerHousePerTimeSlice;
	private Redisson redisson;

	RedissonTest() {
		Config config = new Config();

		// Redisson will use load balance connections between listed servers
		config.addAddress("172.25.187.111:6379");
		redisson = Redisson.create(config);
		averageLoadPerHousePerTimeSlice = redisson.getMap("query1a");
		Calendar cal = Calendar.getInstance();
		long t = 1378604640000l;

		cal.setTimeInMillis(t);
		System.out.println(cal.get(Calendar.HOUR) + ":" + cal.get(Calendar.MINUTE));
		t = t - (24 * 3600 * 1000) + (5 * 2 * 60 * 1000);
		cal.setTimeInMillis(t);
		System.out.println(cal.get(Calendar.HOUR) + ":" + cal.get(Calendar.MINUTE));

	}

	public static void main(String[] args) {
		RedissonTest test = new RedissonTest();
		for (Integer id : test.averageLoadPerHousePerTimeSlice.keySet()) {
			if (id != 4) {
				continue;
			}

			System.out.println(id);
			for (String time : test.averageLoadPerHousePerTimeSlice.get(id).keySet())
				System.out.println(time);
			System.out.println("____________________________________________________________");

		}
		test.redisson.shutdown();

	}
}
