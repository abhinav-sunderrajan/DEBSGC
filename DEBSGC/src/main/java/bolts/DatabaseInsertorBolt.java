package bolts;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import utils.DatabaseAccess;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import beans.SmartPlugBean;

/**
 * This bolt performs a simple operation of committing the live data as is to a
 * database.
 * 
 * @author abhinav
 * 
 */
public class DatabaseInsertorBolt implements IRichBolt {

	private static DatabaseAccess access = new DatabaseAccess();
	private static final Logger LOGGER = Logger.getLogger(DatabaseInsertorBolt.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Initialize with connection details to the database
	 * 
	 * @param connectionProperties
	 */
	public DatabaseInsertorBolt(Properties connectionProperties) {
		access.openDBConnection(connectionProperties);
		attachShutDownHook();

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple input) {
		Object obj = input.getValue(0);
		if (obj instanceof SmartPlugBean) {
			SmartPlugBean bean = (SmartPlugBean) obj;
			access.executeUpdate("INSERT into sensor_data(id,time_stamp,value,property,plug_id,household_id,house_id)"
					+ " VALUES("
					+ bean.getId()
					+ ","
					+ bean.getTimestamp()
					+ ","
					+ bean.getValue()
					+ ","
					+ bean.getProperty()
					+ ","
					+ bean.getPlugId()
					+ ","
					+ bean.getHouseholdId() + "," + bean.getHouseId() + ")");
		}

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	private void attachShutDownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					LOGGER.info("CLose database connection before exit");
					access.closeConnection();
				} catch (SQLException e) {
					LOGGER.error("Error closing database connection", e);
				}
			}
		});
	}

}
