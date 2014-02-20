package utils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;
import org.hyperic.sigar.Cpu;
import org.hyperic.sigar.CpuInfo;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

/**
 * 
 * This thread will be responsible for monitoring and reporting the system
 * parameters at a regular interval. it makes use of the Sigar api.
 * 
 */
@SuppressWarnings("deprecation")
public class SigarSystemMonitor implements Runnable {
	private CpuInfo[] cpuinfo;
	private Cpu cpu;
	private Mem mem;
	private Sigar sigar;
	private static SigarSystemMonitor instance;
	private static final Logger LOGGER = Logger.getLogger(SigarSystemMonitor.class);
	private double cpuUsageScalefactor;
	private static FileWriter writeFile;

	/**
	 * private constructor singleton pattern
	 */

	private SigarSystemMonitor(String imageSaveDirectory) {
		sigar = new Sigar();
		// Settings for display. The code is ugly need to figure out a better

		// End of settings for display
		try {
			cpuinfo = sigar.getCpuInfoList();
			for (int i = 0; i < cpuinfo.length; i++) {
				Map map = cpuinfo[i].toMap();
				LOGGER.info("CPU " + i + ": " + map);
			}

		} catch (SigarException e) {
			LOGGER.error("Error in getting system information from sigar..", e);
		}

	}

	/**
	 * return the instance for the SigarSystemMonitor class
	 * 
	 * @param memFileDir
	 * @param streamRate
	 * @param imageSaveDirectory
	 * @return SigarSystemMonitor
	 */

	public static SigarSystemMonitor getInstance(String memFileDir, int streamRate,
			String imageSaveDirectory) {
		if (instance == null) {
			instance = new SigarSystemMonitor(imageSaveDirectory);
			try {
				writeFile = new FileWriter(memFileDir + "FreeMemPercentage_"
						+ Integer.toString(streamRate) + ".csv");
			} catch (IOException e) {
				LOGGER.error("Error in path provided for memory file", e);
			}
		}

		return instance;

	}

	/**
	 * 
	 * @param cpuUsageScalefactor
	 */
	public void setCpuUsageScalefactor(double cpuUsageScalefactor) {
		this.cpuUsageScalefactor = cpuUsageScalefactor;
	}

	public void run() {

		if (cpuUsageScalefactor == 0.0d) {
			cpuUsageScalefactor = 1000000.0;
		}

		try {
			cpu = sigar.getCpu();
			mem = sigar.getMem();
			long actualFree = mem.getActualFree();
			long actualUsed = mem.getActualUsed();
			long jvmFree = Runtime.getRuntime().freeMemory();
			long jvmTotal = Runtime.getRuntime().totalMemory();

			writeFile.append(Double.toString((jvmFree * 100.0) / jvmTotal));
			writeFile.append("\n");
			writeFile.flush();

			LOGGER.info("System RAM available " + mem.getRam());
			LOGGER.info("Information about the CPU " + cpu.toMap());
			LOGGER.info("Total memory free " + actualFree);
			LOGGER.info("Total memory used " + actualUsed);
			LOGGER.info("JVM free memory " + jvmFree);
			LOGGER.info("JVM total memory " + jvmTotal);

		} catch (SigarException e) {
			LOGGER.error("Error in getting system information from sigar..", e);
		} catch (IOException e) {
			LOGGER.error("Error writing free memory % values to the memory log file", e);
		}
	}
}
