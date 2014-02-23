package streamers;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import main.PlatformCore;

import org.apache.log4j.Logger;
import org.redisson.Redisson;
import org.redisson.core.MessageListener;
import org.redisson.core.RTopic;

import utils.DatabaseAccess;
import beans.HistoryBean;

public class ArchiveLoader<T> implements Runnable {
	protected DatabaseAccess dbconnect = new DatabaseAccess();
	private long startTime;
	private long endTime;
	private int count;
	private Calendar cal;
	private Set<Short> houseIdList;
	private long sliceInMinutes;
	private ConcurrentLinkedQueue<HistoryBean> archiveStreamBufferArr;
	private Redisson redisson;
	private RTopic<Integer> monitor;
	private static final Logger LOGGER = Logger.getLogger(ArchiveLoader.class);

	/**
	 * 
	 * @param buffer
	 * @param monitor
	 * @param startTime
	 * @param rediserverIp
	 */
	public ArchiveLoader(Properties connectionProperties,
			ConcurrentLinkedQueue<HistoryBean> archiveStreamBufferArr, long sliceInMinutes,
			long startTime, String rediserverIp) {
		dbconnect.openDBConnection(connectionProperties);
		this.sliceInMinutes = sliceInMinutes;
		houseIdList = new HashSet<Short>();
		this.startTime = startTime;
		this.endTime = startTime + (sliceInMinutes * 60 * 1000);
		cal = Calendar.getInstance();
		this.archiveStreamBufferArr = archiveStreamBufferArr;

		org.redisson.Config redissonConfig = new org.redisson.Config();
		redissonConfig.addAddress(rediserverIp + ":6379");
		redisson = Redisson.create(redissonConfig);
		monitor = redisson.getTopic("monitor");
	}

	@Override
	public void run() {
		ResultSet rs = dbconnect
				.retrieveQueryResult("SELECT AVG(value) AS avgLoad, count(*) as counts,house_id,household_id"
						+ ",plug_id FROM sensor_data_archive WHERE time_stamp >="
						+ startTime
						+ " AND time_stamp < "
						+ endTime
						+ " AND property="
						+ PlatformCore.LOAD_PROPERTY
						+ " GROUP BY house_id,household_id,plug_id ORDER BY house_id,household_id,plug_id");
		try {
			String timeSlice = null;
			while (rs.next()) {

				HistoryBean bean = new HistoryBean();
				bean.setAverageLoad(rs.getFloat("avgLoad"));
				bean.setReadingsCount(rs.getInt("counts"));
				bean.setHouseId(rs.getShort("house_id"));
				bean.setHouseholdId(rs.getShort("household_id"));
				bean.setPlugId(rs.getShort("plug_id"));
				houseIdList.add(bean.getHouseId());

				cal.setTimeInMillis(startTime);
				int hrs = cal.get(Calendar.HOUR);
				int mnts = cal.get(Calendar.MINUTE);
				String predTimeStart = String.format("%02d:%02d", hrs, mnts);
				cal.setTimeInMillis(endTime);
				hrs = cal.get(Calendar.HOUR);
				mnts = cal.get(Calendar.MINUTE);
				String predTimeEnd = String.format("%02d:%02d", hrs, mnts);
				timeSlice = predTimeStart + " TO " + predTimeEnd;
				bean.setTimeSlice(timeSlice);

				archiveStreamBufferArr.add(bean);
			}
			// Create a punctuation event for each house id after each database
			// load

			for (short houseId : houseIdList) {
				HistoryBean punctuation = new HistoryBean(houseId, (short) -1, (short) -1, -1.0f,
						1, timeSlice);
				archiveStreamBufferArr.add(punctuation);
			}
			houseIdList.clear();
			// End of punctuation events

			count++;
			if (count == 1) {
				LOGGER.info("Wait for live streams before further database loading");
				final List<Integer> wait = new ArrayList<>();

				monitor.addListener(new MessageListener<Integer>() {

					public void onMessage(Integer message) {
						wait.add(1);
					}
				});

				while (wait.size() == 0) {
					// very bad implementation but do not how else
					Thread.sleep(500);
				}

				LOGGER.info("Receiving live streams. Start database load normally");
			}

			// Update the time stamps for the next fetch.
			startTime = startTime + (sliceInMinutes * 60 * 1000);
			endTime = endTime + (sliceInMinutes * 60 * 1000);

		} catch (SQLException e) {
			LOGGER.error("Error accessing the database to retrieve archived data", e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
