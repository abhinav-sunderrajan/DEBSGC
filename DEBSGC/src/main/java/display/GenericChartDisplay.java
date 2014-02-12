package display;

import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import main.PlatformCore;

import org.apache.batik.dom.GenericDOMImplementation;
import org.apache.batik.svggen.SVGGraphics2D;
import org.apache.log4j.Logger;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.ui.ApplicationFrame;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;

@SuppressWarnings("serial")
/**
 * 
 * Creates the common settings for a  JFree chart.
 *
 */
public abstract class GenericChartDisplay extends ApplicationFrame {

	private JFreeChart chart;
	protected ChartPanel chartPanel;
	protected String title;
	protected static Object lock;
	protected static String directory;
	private static final Logger LOGGER = Logger.getLogger(GenericChartDisplay.class);

	/**
	 * 
	 * @param timeseriesList
	 * @param imageSaveDirectory
	 */
	public GenericChartDisplay(final String title, final String imageSaveDirectory) {

		super(title);
		this.title = title;
		directory = imageSaveDirectory;
		lock = new Object();
		PlatformCore.executor.scheduleAtFixedRate(new LazySave(), 5, 5, TimeUnit.MINUTES);

	}

	/**
	 * Creates the settings for the time-series display can be overridden if the
	 * sub class deems necessary.
	 */
	protected abstract void settings();

	public JFreeChart getChart() {
		return chart;
	}

	public void setChart(JFreeChart chart) {
		this.chart = chart;
	}

	private class LazySave implements Runnable {
		DateFormat df;

		public LazySave() {
			df = new SimpleDateFormat("MM-dd-yyyy HH-mm-ss");

		}

		@Override
		public void run() {
			File svgFile = new File(directory + title + "_" + df.format(new Date()) + ".svg");
			DOMImplementation domImpl = GenericDOMImplementation.getDOMImplementation();
			Document document = domImpl.createDocument(null, "svg", null);

			// Create an instance of the SVG Generator
			SVGGraphics2D svgGenerator = new SVGGraphics2D(document);

			// draw the chart in the SVG generator
			getChart().draw(svgGenerator, new Rectangle2D.Double(0, 0, 1024, 600));

			// Write svg file
			OutputStream outputStream;
			try {
				outputStream = new FileOutputStream(svgFile);

				Writer out = new OutputStreamWriter(outputStream, "UTF-8");
				svgGenerator.stream(out, true /* use css */);
				outputStream.flush();
				outputStream.close();
			} catch (Exception e) {
				LOGGER.error("Error creating SVG image", e);
			}

		}
	}

}
