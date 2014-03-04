package bolts;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import utils.DatabaseAccess;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import beans.SmartPlugBean;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * This bolt performs a simple operation of committing the live data as is to a
 * database. The insert to the database is performed in batches of size 5000.
 * 
 * @author abhinav
 * 
 */
public class DatabaseInsertorBolt implements IRichBolt {

	private transient DatabaseAccess access;
	private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();
	private transient Disruptor<SmartPlugBean> disruptor;
	private RingBuffer<SmartPlugBean> ringBuffer;
	private final static int RING_SIZE = 262144;
	private EventHandler<SmartPlugBean> handler;
	private Connection connect;
	private PreparedStatement ps;
	private static String sql;
	private static final Logger LOGGER = Logger.getLogger(DatabaseInsertorBolt.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		access = new DatabaseAccess();

		String url = (String) stormConf.get("database.url");
		String dbName = (String) stormConf.get("database.name");
		String userName = (String) stormConf.get("database.username");
		String password = (String) stormConf.get("database.password");

		// Initialize the handler first
		try {
			this.connect = access.openDBConnection(url, dbName, userName, password);
			sql = "INSERT into sensor_data_archive_7(id,time_stamp,value,property,plug_id,household_id,house_id)"
					+ " VALUES(?,?,?,?,?,?,?)";
			ps = connect.prepareStatement(sql);
			implementLMAXHandler();
		} catch (SQLException e) {
			LOGGER.error("Error creating database insert handler check the insert query", e);
		}
		disruptor = new Disruptor<SmartPlugBean>(SmartPlugBean.EVENT_FACTORY, EXECUTOR,
				new SingleThreadedClaimStrategy(RING_SIZE), new SleepingWaitStrategy());
		disruptor.handleEventsWith(handler);
		ringBuffer = disruptor.start();

	}

	private void implementLMAXHandler() throws SQLException {

		// The event handler to execute a batch insert to database.
		handler = new EventHandler<SmartPlugBean>() {
			final int batchSize = 5000;
			int count = 0;

			public void onEvent(final SmartPlugBean bean, final long sequence,
					final boolean endOfBatch) throws Exception {
				ps.setLong(1, bean.getId());
				ps.setLong(2, bean.getTimestamp());
				ps.setFloat(3, bean.getValue());
				ps.setShort(4, bean.getProperty());
				ps.setShort(5, bean.getPlugId());
				ps.setShort(6, bean.getHouseholdId());
				ps.setShort(7, bean.getHouseId());
				ps.addBatch();

				if (++count % batchSize == 0) {
					ps.executeBatch();
				}

			}
		};

	}

	@Override
	public void execute(Tuple input) {
		Object obj = input.getValue(0);
		if (obj instanceof SmartPlugBean) {
			SmartPlugBean bean = (SmartPlugBean) obj;
			long sequence = ringBuffer.next();
			SmartPlugBean next = ringBuffer.get(sequence);
			next.setHouseholdId(bean.getHouseholdId());
			next.setHouseId(bean.getHouseId());
			next.setId(bean.getId());
			next.setPlugId(bean.getPlugId());
			next.setProperty(bean.getProperty());
			next.setTimestamp(bean.getTimestamp());
			next.setValue(bean.getValue());
			ringBuffer.publish(sequence);

		}

	}

	@Override
	public void cleanup() {
		try {
			LOGGER.info("Committing residual records before insert");
			ps.executeBatch();
			LOGGER.info("Close database connection before exit");
			ps.close();
			access.closeConnection();
		} catch (SQLException e) {
			LOGGER.error("Error closing database connection", e);
		}

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

}
