package main;

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

	}

	public static void main(String[] args) {
		RedissonTest test = new RedissonTest();
		for (Integer id : test.averageLoadPerHousePerTimeSlice.keySet()) {
			System.out.println(id);
			System.out.println(test.averageLoadPerHousePerTimeSlice.get(id));
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

		}
		test.redisson.shutdown();

	}
}
