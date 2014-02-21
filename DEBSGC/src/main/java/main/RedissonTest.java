package main;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.redisson.Config;
import org.redisson.Redisson;

import utils.CircularList;

public class RedissonTest {

	public static void main(String[] args) {
		Config config = new Config();
		config.setConnectionPoolSize(10);

		// Redisson will use load balance connections between listed servers
		config.addAddress("172.25.187.111:6379");
		Redisson redisson = Redisson.create(config);
		Map<Integer, ConcurrentHashMap<String, CircularList<Double>>> averageLoadPerHousePerTimeSlice = redisson
				.getMap("query1a");

		// ArrayList<Integer> al1 = new ArrayList<Integer>();
		// ArrayList<Integer> al2 = new ArrayList<Integer>();
		// for (int i = 1; i < 31; i++) {
		// al1.add(i);
		// al2.add(i * 999);
		//
		// }
		//
		// map.put("al1", al1);
		// map.put("al2", al2);

		for (Integer key : averageLoadPerHousePerTimeSlice.keySet()) {
			System.out.println("houseId:" + key);
			System.out.println("timeslots: " + averageLoadPerHousePerTimeSlice.get(key));
		}

		redisson.shutdown();

	}
}
