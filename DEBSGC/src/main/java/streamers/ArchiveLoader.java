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
	protected DatabaseAccess dbconnect = new DatabaseAccess();
	protected Object monitor;
	private long startTime;
	private long endTime;
	private int count;
	private Calendar cal;
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
		cal = Calendar.getInstance();
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

				cal.setTimeInMillis(startTime);
				int hrs = cal.get(Calendar.HOUR);
				int mnts = cal.get(Calendar.MINUTE);
				int secs = cal.get(Calendar.SECOND);
				String predTimeStart = String.format("%02d:%02d:%02d", hrs, mnts, secs);
				cal.setTimeInMillis(endTime);
				hrs = cal.get(Calendar.HOUR);
				mnts = cal.get(Calendar.MINUTE);
				secs = cal.get(Calendar.SECOND);
				String predTimeEnd = String.format("%02d:%02d:%02d", hrs, mnts, secs);
				timeSlice = predTimeStart + " TO " + predTimeEnd;
				bean.setTimeSlice(timeSlice);
				buffer.add((T) bean);

			}
			// Create a punctuation event after each database load
			HistoryBean punctuation = new HistoryBean((short) -1, (short) -1, (short) -1, 0.0f, 1,
					timeSlice);
			buffer.add((T) punctuation);

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
