package streamers;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import main.PlatformCore;

import org.apache.log4j.Logger;

import utils.DatabaseAccess;
import beans.HistoryBean;

public class ArchiveLoader<T> implements Runnable {
	private Queue<T> buffer;
	protected DatabaseAccess dbconnect;
	protected Object monitor;
	private long startTime;
	private long endTime;
	private int count;
	private Calendar calStart;
	private Calendar calEnd;
	private static final Logger LOGGER = Logger.getLogger(ArchiveLoader.class);

	/**
	 * 
	 * @param buffer
	 * @param monitor
	 * @param startTime
	 */
	public ArchiveLoader(final ConcurrentLinkedQueue<T> buffer, final Object monitor,
			final long startTime) {
		this.buffer = buffer;
		dbconnect.openDBConnection(PlatformCore.connectionProperties);
		this.monitor = monitor;
		this.startTime = startTime;
		this.endTime = startTime + (PlatformCore.SLICE_IN_MINUTES * 60 * 1000);
		calStart = Calendar.getInstance();
		calEnd = Calendar.getInstance();
	}

	@Override
	public void run() {
		ResultSet rs = dbconnect
				.retrieveQueryResult("SELECT AVG(value) AS avgLoad, house_id,household_id,plug_id FROM sensor_data "
						+ "WHERE time_stamp >="
						+ startTime
						+ " AND time_stamp < "
						+ endTime
						+ "AND property=1 GROUP BY house_id,household_id,plug_id");
		try {
			while (rs.next()) {

				HistoryBean bean = new HistoryBean();
				bean.setAverageLoad(rs.getFloat("avgLoad"));
				bean.setHouseId(rs.getShort("house_id"));
				bean.setHouseholdId(rs.getShort("household_id"));
				bean.setPlugId(rs.getShort("plug_id"));
				calStart.setTimeInMillis(startTime);
				calEnd.setTimeInMillis(endTime);
				String timeSlice = calStart.get(Calendar.HOUR_OF_DAY) + ":"
						+ calStart.get(Calendar.MINUTE) + ":" + calStart.get(Calendar.SECOND)
						+ " TO " + calEnd.get(Calendar.HOUR_OF_DAY) + ":"
						+ calEnd.get(Calendar.MINUTE) + ":" + calEnd.get(Calendar.SECOND);

				bean.setTimeSlice(timeSlice);
				buffer.add((T) bean);

			}

			if (count == 2) {
				synchronized (monitor) {
					LOGGER.info("Wait for live streams before further database loading");
					monitor.wait();
					LOGGER.info("Receiving live streams. Start database load normally");
				}
			}

			// Update the time stamps for the next fetch.
			startTime = startTime + (PlatformCore.SLICE_IN_MINUTES * 60 * 1000);
			endTime = endTime + (PlatformCore.SLICE_IN_MINUTES * 60 * 1000);
			count++;
		} catch (SQLException e) {
			LOGGER.error("Error accessing the database to retrieve archived data", e);
		} catch (InterruptedException e) {
			LOGGER.error("Error waking the sleeping threads", e);
		}

	}

}
