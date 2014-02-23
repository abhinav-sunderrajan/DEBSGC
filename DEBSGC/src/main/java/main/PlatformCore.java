package main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.log4j.Logger;
import org.redisson.Redisson;

import spouts.ArchiveStreamSpout;
import spouts.LiveStreamSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import beans.HistoryBean;
import beans.SmartPlugBean;
import bolts.ArchiveMedianPerHouseBolt;
import bolts.ArchiveMedianPerPlugBolt;
import bolts.CurrentLoadAvgPerHouseBolt;
import bolts.CurrentLoadAvgPerPlugBolt;
import bolts.Query1ALiveArchiveJoin;
import bolts.Query1BLiveArchiveJoin;
import bolts.StreamProviderQuery1A;
import bolts.StreamProviderQuery1B;

/**
 * A singleton instance of the stream processing platform to set up the
 * environment.
 * 
 * @author abhinav.sunderrajan
 * 
 */
public class PlatformCore {

	private TopologyBuilder builder;
	private static long archiveStartTime;
	private static PlatformCore core;
	private static int streamRate;
	private static final int SERVER_PORT = 9090;
	private static final String CONFIG_FILE_PATH = "src/main/resources/config.properties";
	private static final String CONNECTION_FILE_PATH = "src/main/resources/connection.properties";
	private static int dbLoadRate;
	private static long liveStartTime;
	public static ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
	private static Properties connectionProperties;
	private static Properties configProperties;
	private static final Integer SLICE_IN_MINUTES = 1;
	public static final Integer WORK_PROPERTY = 0;
	public static final Integer LOAD_PROPERTY = 1;
	private static final Integer NUMBER_OF_ARCHIVE_STREAMS = 1;
	private static final Integer NUMBER_OF_DAYS_IN_ARCHIVE = 29;
	private static final Logger LOGGER = Logger.getLogger(PlatformCore.class);
	public static final DateFormat df = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
	// Lock for coordinating the archive loader and the live streams.
	private static Redisson redisson;

	private PlatformCore() throws ParseException {
		org.redisson.Config redissonConfig = new org.redisson.Config();
		// Redisson will use load balance connections between listed servers
		redissonConfig.addAddress(configProperties.getProperty("redis.server") + ":6379");
		redisson = Redisson.create(redissonConfig);
		redisson.flushdb();

		streamRate = Integer
				.parseInt(configProperties.getProperty("live.stream.rate.in.microsecs"));
		dbLoadRate = (int) (1000 * 60 * SLICE_IN_MINUTES * streamRate / 1000000);
		LOGGER.info("Database load rate in seconds is " + dbLoadRate);
		builder = new TopologyBuilder();
		liveStartTime = Long.parseLong((PlatformCore.configProperties
				.getProperty("live.start.time")));
		archiveStartTime = liveStartTime - (24 * 3600 * 1000) + (SLICE_IN_MINUTES * 2 * 60 * 1000);

	}

	public static void main(String[] args) {

		String configFilePath;
		String connectionFilePath;
		String topologyName = null;
		boolean isLocal = true;

		if (args.length < 4) {
			configFilePath = CONFIG_FILE_PATH;
			connectionFilePath = CONNECTION_FILE_PATH;
			topologyName = "test";
		} else {
			configFilePath = args[0];
			connectionFilePath = args[1];
			topologyName = args[2];
			isLocal = Boolean.valueOf(args[3]);
		}

		connectionProperties = new Properties();
		configProperties = new Properties();
		try {
			configProperties.load(new FileInputStream(configFilePath));
			connectionProperties.load(new FileInputStream(connectionFilePath));
			core = new PlatformCore();

			Config conf = new Config();
			conf.setNumWorkers(5);
			conf.setDebug(false);
			for (Entry<Object, Object> entry : configProperties.entrySet()) {
				conf.put((String) entry.getKey(), entry.getValue());

			}
			for (Entry<Object, Object> entry : connectionProperties.entrySet()) {
				conf.put((String) entry.getKey(), entry.getValue());

			}

			conf.put("dbLoadRate", dbLoadRate);
			conf.put("NUMBER_OF_DAYS_IN_ARCHIVE", NUMBER_OF_DAYS_IN_ARCHIVE);
			conf.put("SLICE_IN_MINUTES", SLICE_IN_MINUTES);
			conf.put("LOAD_PROPERTY", LOAD_PROPERTY);
			conf.put("WORK_PROPERTY", WORK_PROPERTY);

			// Start monitoring the system CPU, memory parameters
			// SigarSystemMonitor sysMonitor = SigarSystemMonitor.getInstance(
			// configProperties.getProperty("memory.file.dir"), streamRate,
			// configProperties.getProperty("image.save.directory"));
			// sysMonitor.setCpuUsageScalefactor((Double.parseDouble(configProperties
			// .getProperty("cpu.usage.scale.factor"))));
			// executor.scheduleAtFixedRate(sysMonitor, 0, 30,
			// TimeUnit.SECONDS);

			// Bolt for computing the median from the archive streams
			long archiveloadStart = archiveStartTime;
			BoltDeclarer declarer = core.builder
					.setBolt("ArchiveMedianPerPlugBolt", new ArchiveMedianPerPlugBolt(new Fields(
							"HistoryBean", "houseid", "timeSlice")), 5);

			for (int count = 0; count < NUMBER_OF_ARCHIVE_STREAMS; count++) {

				ArchiveStreamSpout<HistoryBean> archiveStreamSpout = new ArchiveStreamSpout<HistoryBean>(
						new ConcurrentLinkedQueue<HistoryBean>(), connectionProperties,
						archiveloadStart);
				core.builder.setSpout("archive_stream_" + count, archiveStreamSpout);
				declarer.fieldsGrouping("archive_stream_" + count, new Fields("houseId",
						"householdId", "plugId", "timeSlice"));
				archiveloadStart = archiveloadStart + 24 * 3600 * 1000;

			}

			core.builder.setBolt("ArchiveMedianPerHouseBolt", new ArchiveMedianPerHouseBolt(), 5)
					.fieldsGrouping("ArchiveMedianPerPlugBolt", new Fields("houseid", "timeSlice"));

			// After 24 hours all but one archive thread can be cancelled since
			// the median values for other time slices are stored in memory.

			LiveStreamSpout<SmartPlugBean> liveStreamLoadSpout = new LiveStreamSpout<SmartPlugBean>(
					new ConcurrentLinkedQueue<SmartPlugBean>(), executor, streamRate, SERVER_PORT,
					new Fields("livebean", "houseId", "householdId", "plugId"));

			core.builder.setSpout("live_stream", liveStreamLoadSpout);

			// // Save the Live streams to the database as and when it comes.
			// core.builder.setBolt("save_to_archive", new
			// DatabaseInsertorBolt(connectionProperties),
			// 5).shuffleGrouping("live_stream");

			// Topology for query 1a
			core.builder.setBolt(
					"CurrentLoadAvgPerHouseBolt",
					new CurrentLoadAvgPerHouseBolt(SLICE_IN_MINUTES * 60000, new Fields("houseId",
							"CurrentLoadPerHouseBean")), 10).fieldsGrouping("live_stream",
					new Fields("houseId"));

			core.builder.setBolt(
					"Query1ALiveArchiveJoin",
					new Query1ALiveArchiveJoin(new Fields("houseId", "currentLoad",
							"predictedLoad", "predictedTimeString", "evalTime")), 5)
					.fieldsGrouping("CurrentLoadAvgPerHouseBolt", new Fields("houseId"));
			core.builder.setBolt(
					"StreamProviderQuery1A",
					new StreamProviderQuery1A(
							configProperties.getProperty("query1a.subscriber.ip"), Integer
									.parseInt(configProperties
											.getProperty("query1a.subscriber.port"))), 1)
					.globalGrouping("Query1ALiveArchiveJoin");

			// Topology for query 1b
			core.builder.setBolt(
					"CurrentLoadAvgPerPlugBolt",
					new CurrentLoadAvgPerPlugBolt(SLICE_IN_MINUTES * 60000, new Fields("houseId",
							"householdId", "plugId", "CurrentLoadPerPlugBean")), 5).fieldsGrouping(
					"live_stream", new Fields("houseId", "householdId", "plugId"));
			core.builder.setBolt(
					"Query1BLiveArchiveJoin",
					new Query1BLiveArchiveJoin(new Fields("houseId_householdId_plugId",
							"currentLoad", "predictedLoad", "predictedTimeString", "evalTime")), 5)
					.fieldsGrouping("CurrentLoadAvgPerPlugBolt",
							new Fields("houseId", "householdId", "plugId"));
			core.builder.setBolt(
					"StreamProviderQuery1B",
					new StreamProviderQuery1B(
							configProperties.getProperty("query1b.subscriber.ip"), Integer
									.parseInt(configProperties
											.getProperty("query1b.subscriber.port"))), 1)
					.globalGrouping("Query1BLiveArchiveJoin");

			// Topology for query 2
			//
			// String[] medianBoltEvents = { SmartPlugBean.class.getName() };
			//
			// // Emits the median load for all plugs over window of the
			// // specified size
			// core.builder.setBolt(
			// "GlobalMedianLoadBolt",
			// new GlobalMedianLoadBolt(medianBoltEvents, ProjectUtils
			// .getGlobalMedianLoadPerHour(), "GlobalMedianLoadBolt", new
			// Fields(
			// "livebean", "houseId", "householdId", "plugId")),
			// 1).globalGrouping(
			// "live_stream");
			//
			// // Calculates the median load per plug
			// core.builder.setBolt(
			// "PerPlugMedianBolt",
			// new PerPlugMedianBolt(new Fields("medianLoad", "globalMedian",
			// "timestampStart", "timestampEnd", "queryEvalTime", "houseId",
			// "percentage", "latency")),
			// 5).fieldsGrouping("GlobalMedianLoadBolt",
			// new Fields("houseId", "householdId", "plugId"));
			//
			// // Compares the median load of the plug with the global median
			// // before estimating the outliers per house.
			// core.builder.setBolt(
			// "Query2AOutlierEstimator",
			// new Query2AOutlierEstimator(new Fields("timeFrame", "houseId",
			// "percentage",
			// "latency")), 5).shuffleGrouping("PerPlugMedianBolt");
			//
			// // Finally send to display
			//
			// core.builder
			// .setBolt(
			// "StreamProviderQuery2",
			// new StreamProviderQuery2(configProperties
			// .getProperty("query2.subscriber.ip"), Integer
			// .parseInt(configProperties
			// .getProperty("query2.subscriber.port"))))
			// .globalGrouping("Query2AOutlierEstimator");

			if (isLocal) {
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology(topologyName, conf, core.builder.createTopology());
			} else {

				conf.put(Config.NIMBUS_CHILDOPTS, "-Xmx2048m -Xms2048m  "
						+ "-Xloggc:logs/gc1k2.log -XX:+PrintGCDetails -XX:+UseParallelOldGC "
						+ "-XX:GCTimeRatio=4 -XX:NewRatio=2");
				StormSubmitter.submitTopology(topologyName, conf, core.builder.createTopology());
			}

			// Utils.sleep(100000);
			// cluster.killTopology("test");
			// cluster.shutdown();

		} catch (FileNotFoundException e) {
			LOGGER.error("Unable to find the config/connection properties files", e);
		} catch (IOException e) {
			LOGGER.error("Properties file contains non unicode values ", e);

		} catch (ParseException e) {
			LOGGER.error("Error parsing archive stream start time", e);
		} catch (AlreadyAliveException e) {
			LOGGER.error("Error submitting topology to cluster", e);
		} catch (InvalidTopologyException e) {
			LOGGER.error("Error submitting topology to cluster", e);
		}

	}
}
