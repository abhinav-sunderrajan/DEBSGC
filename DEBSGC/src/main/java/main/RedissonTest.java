package main;

import java.util.Calendar;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import org.redisson.Config;
import org.redisson.Redisson;

public class RedissonTest {

	private ConcurrentMap<Integer, ConcurrentMap<String, Boolean>> houseIdMap;
	private Redisson redisson;

	RedissonTest() {
		Config config = new Config();

		// Redisson will use load balance connections between listed servers
		config.addAddress("localhost:6379");
		redisson = Redisson.create(config);
		houseIdMap = redisson.getMap("query2a");
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
		for (Integer id : test.houseIdMap.keySet()) {

			System.out.println("<<<<< " + id + " >>>>>");
			for (Entry<String, Boolean> entry : test.houseIdMap.get(id).entrySet())
				System.out.println(entry.getKey() + "<<>>" + entry.getValue());
			System.out.println("____________________________________________________________");

		}
		test.redisson.shutdown();

	}
}
