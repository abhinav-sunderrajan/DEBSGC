package main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.Buffer;
import org.apache.log4j.Logger;

import spouts.ArchiveStreamSpout;
import spouts.LiveStreamSpout;
import streamers.ArchiveLoader;
import utils.SigarSystemMonitor;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import beans.HistoryBean;
import beans.SmartPlugBean;
import bolts.ArchiveMedianPerHouseBolt;
import bolts.ArchiveMedianPerPlugBolt;
import bolts.CurrentLoadAvgPerHouseBolt;
import bolts.DisplayBoltQuery1A;
import bolts.Query1ALiveArchiveJoin;

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
	private static int dbLoadRate;
	private static int archiveStreamRate;
	private Integer monitor;
	private static final int SERVER_PORT = 9090;
	private static final String CONFIG_FILE_PATH = "src/main/resources/config.properties";
	private static final String CONNECTION_FILE_PATH = "src/main/resources/connection.properties";
	public static long liveStartTime;
	public static ScheduledExecutorService executor = Executors.newScheduledThreadPool(100);
	public static Properties connectionProperties;
	public static Properties configProperties;
	public static final int SLICE_IN_MINUTES = 15;
	public static final int WORK_PROPERTY = 0;
	public static final int LOAD_PROPERTY = 1;
	public static final int NUMBER_OF_ARCHIVE_STREAMS = 1;
	public static final int NUMBER_OF_DAYS_IN_ARCHIVE = 29;
	private static final Logger LOGGER = Logger.getLogger(PlatformCore.class);
	public static final DateFormat df = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");

	// A map maintaining a list of average load values per time slice per house.
	// The size of the Circular buffer is determined by the number of days in
	// history we are interested in.
	public static Map<Short, ConcurrentHashMap<String, Buffer>> averageLoadPerHousePerTimeSlice = new ConcurrentHashMap<Short, ConcurrentHashMap<String, Buffer>>();
	public static Map<String, ConcurrentHashMap<String, Buffer>> averageLoadPerPlugPerTimeSlice = new ConcurrentHashMap<String, ConcurrentHashMap<String, Buffer>>();

	// Data structure for query 2

	public static Map<Short, ConcurrentHashMap<String, Boolean>> loadStatusMap = new ConcurrentHashMap<Short, ConcurrentHashMap<String, Boolean>>();

	private PlatformCore() throws ParseException {
		monitor = new Integer(0);
		streamRate = Integer
				.parseInt(configProperties.getProperty("live.stream.rate.in.microsecs"));
		dbLoadRate = (int) (SLICE_IN_MINUTES * 0.4);
		archiveStreamRate = (int) (streamRate * 0.9);
		builder = new TopologyBuilder();
		liveStartTime = Long.parseLong((PlatformCore.configProperties
				.getProperty("live.start.time")));
		archiveStartTime = liveStartTime - (24 * 3600 * 1000) + (SLICE_IN_MINUTES * 2 * 60 * 1000);

	}

	public static void main(String[] args) {

		String configFilePath;
		String connectionFilePath;

		if (args.length < 2) {
			configFilePath = CONFIG_FILE_PATH;
			connectionFilePath = CONNECTION_FILE_PATH;
		} else {
			configFilePath = args[0];
			connectionFilePath = args[1];
		}

		connectionProperties = new Properties();
		configProperties = new Properties();
		try {
			configProperties.load(new FileInputStream(configFilePath));
			connectionProperties.load(new FileInputStream(connectionFilePath));
			core = new PlatformCore();

			// Start monitoring the system CPU, memory parameters
			SigarSystemMonitor sysMonitor = SigarSystemMonitor.getInstance(
					configProperties.getProperty("memory.file.dir"), streamRate,
					configProperties.getProperty("image.save.directory"));
			sysMonitor.setCpuUsageScalefactor((Double.parseDouble(configProperties
					.getProperty("cpu.usage.scale.factor"))));
			executor.scheduleAtFixedRate(sysMonitor, 0, 30, TimeUnit.SECONDS);

			// Bolt for computing the median from the archive streams
			long archiveloadStart = archiveStartTime;
			BoltDeclarer declarer = core.builder
					.setBolt("ArchiveMedianPerPlugBolt", new ArchiveMedianPerPlugBolt(new Fields(
							"HistoryBean", "houseid", "timeSlice")), 5);

			for (int count = 0; count < NUMBER_OF_ARCHIVE_STREAMS; count++) {

				ConcurrentLinkedQueue<HistoryBean> archiveStreamBuffer = new ConcurrentLinkedQueue<HistoryBean>();
				ArchiveLoader<HistoryBean> archiveLoader = new ArchiveLoader<HistoryBean>(
						archiveStreamBuffer, core.monitor, archiveloadStart);
				ScheduledFuture<?> future = executor.scheduleAtFixedRate(archiveLoader, 0,
						dbLoadRate, TimeUnit.MINUTES);
				ArchiveStreamSpout<HistoryBean> archiveStreamSpout = new ArchiveStreamSpout<HistoryBean>(
						archiveStreamBuffer, archiveStreamRate);
				core.builder.setSpout("archive_stream_" + count, archiveStreamSpout);
				declarer.fieldsGrouping("archive_stream_" + count, new Fields("houseId",
						"householdId", "plugId", "timeSlice"));
				archiveloadStart = archiveloadStart + 24 * 3600 * 1000;

			}

			core.builder.setBolt("ArchiveMedianPerHouseBolt", new ArchiveMedianPerHouseBolt(), 1)
					.fieldsGrouping("ArchiveMedianPerPlugBolt", new Fields("houseid", "timeSlice"));

			// After 24 hours all but one archive thread can be cancelled since
			// the median values for other time slices are stored in memory.

			ConcurrentLinkedQueue<SmartPlugBean> liveStreamBuffer = new ConcurrentLinkedQueue<SmartPlugBean>();

			LiveStreamSpout<SmartPlugBean> liveStreamLoadSpout = new LiveStreamSpout<SmartPlugBean>(
					liveStreamBuffer, core.monitor, executor, streamRate, SERVER_PORT,
					configProperties.getProperty("ingestion.file.dir"),
					configProperties.getProperty("image.save.directory"), new Fields("livebean",
							"houseId", "householdId", "plugId"));

			core.builder.setSpout("live_stream", liveStreamLoadSpout);

			// // Save the Live streams to the database as and when it comes.
			// core.builder.setBolt("save_to_archive", new
			// DatabaseInsertorBolt(connectionProperties),
			// 5).shuffleGrouping("live_stream");

			// Topology for query 1a
			core.builder.setBolt(
					"CurrentLoadAvgPerHouseBolt",
					new CurrentLoadAvgPerHouseBolt(Long.parseLong(configProperties
							.getProperty("query1.window.size.ms")), new Fields("houseId",
							"CurrentLoadPerHouseBean")), 5).fieldsGrouping("live_stream",
					new Fields("houseId"));

			core.builder.setBolt(
					"Query1ALiveArchiveJoin",
					new Query1ALiveArchiveJoin(new Fields("houseId", "currentLoad",
							"predictedLoad", "predictedTimeString", "evalTime")), 5)
					.fieldsGrouping("CurrentLoadAvgPerHouseBolt", new Fields("houseId"));
			core.builder.setBolt("DisplayBoltQuery1A", new DisplayBoltQuery1A(streamRate), 1)
					.globalGrouping("Query1ALiveArchiveJoin");

			// Topology for query 1b
			// core.builder.setBolt(
			// "CurrentLoadAvgPerPlugBolt",
			// new CurrentLoadAvgPerPlugBolt(Long.parseLong(configProperties
			// .getProperty("query1.window.size.ms")), new Fields("houseId",
			// "householdId", "plugId", "CurrentLoadPerPlugBean")),
			// 5).fieldsGrouping(
			// "live_stream", new Fields("houseId", "householdId", "plugId"));
			// core.builder.setBolt(
			// "Query1BLiveArchiveJoin",
			// new Query1BLiveArchiveJoin(new Fields("houseId", "householdId",
			// "plugId",
			// "currentLoad", "predictedLoad", "predictedTimeString",
			// "evalTime")), 5)
			// .fieldsGrouping("CurrentLoadAvgPerPlugBolt",
			// new Fields("houseId", "householdId", "plugId"));

			// Topology for query 2

			// String[] medianBoltEvents = { SmartPlugBean.class.getName() };
			//
			// // Emits the median load for all plugs over window of the
			// // specified size
			// core.builder.setBolt(
			// "GlobalMedianLoadBolt",
			// new GlobalMedianLoadBolt(medianBoltEvents, EsperQueries
			// .getGlobalMedianLoadPerHour(), "GlobalMedianLoadBolt", new
			// Fields(
			// "livebean", "houseId", "householdId", "plugId")),
			// 1).globalGrouping(
			// "live_stream");
			//
			// // Calculates the median load per plug
			// core.builder.setBolt(
			// "PerHouseMedianBolt",
			// new PerHouseMedianBolt(new Fields("medianLoad", "globalMedian",
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
			// "latency")), 5).shuffleGrouping("PerHouseMedianBolt");
			//
			// // Finally send to display
			//
			// core.builder.setBolt("DisplayBoltQuery2", new
			// DisplayBoltQuery2()).globalGrouping(
			// "Query2AOutlierEstimator");

			Config conf = new Config();
			conf.setNumWorkers(2);
			conf.setDebug(false);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, core.builder.createTopology());
			// Utils.sleep(100000);
			// cluster.killTopology("test");
			// cluster.shutdown();

		} catch (FileNotFoundException e) {
			LOGGER.error("Unable to find the config/connection properties files", e);
		} catch (IOException e) {
			LOGGER.error("Properties file contains non unicode values ", e);

		} catch (ParseException e) {
			LOGGER.error("Error parsing archive stream start time", e);
		}

	}
}
