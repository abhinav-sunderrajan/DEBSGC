package display;

import java.awt.BasicStroke;
import java.awt.Color;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.labels.StandardXYItemLabelGenerator;
import org.jfree.chart.labels.XYItemLabelGenerator;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.time.Minute;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.ui.RectangleInsets;
import org.jfree.ui.RefineryUtilities;

/**
 * Extended for a time series JFree Chart
 * 
 * @author abhinav
 * 
 */
public class TimeSeriesDisplay extends GenericChartDisplay {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private TimeSeriesCollection dataset;
	private Map<Integer, TimeSeries> timeSeriesMap;
	private XYLineAndShapeRenderer renderer;

	public TimeSeriesDisplay(String title, String imageSaveDirectory, TimeSeriesCollection dataset) {
		super(title, imageSaveDirectory);
		this.dataset = dataset;
		timeSeriesMap = new HashMap<Integer, TimeSeries>();
		chart = ChartFactory.createTimeSeriesChart(title, "Time", "Value", dataset, true, true,
				false);
		settings();
	}

	/**
	 * Refresh all the time series values
	 * 
	 * @param values
	 */
	public synchronized void refreshDisplayValues(Map<Integer, Double> values) {
		Iterator<Entry<Integer, Double>> it = values.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pairs = (Map.Entry) it.next();
			timeSeriesMap.get(pairs.getKey()).addOrUpdate(new Minute(), (Double) pairs.getValue());
		}
	}

	/**
	 * Add a time series to the display with a unique key.
	 * 
	 * @param series
	 * @param key
	 */
	public synchronized void addToDataSeries(TimeSeries series, int key) {
		dataset.addSeries(series);
		timeSeriesMap.put(key, series);

	}

	@Override
	protected void settings() {

		chartPanel = new ChartPanel(chart);
		chartPanel.setPreferredSize(new java.awt.Dimension(1000, 500));
		chartPanel.setMouseZoomable(true, false);
		setContentPane(chartPanel);
		XYPlot plot = chart.getXYPlot();
		plot.setBackgroundPaint(Color.WHITE);
		plot.setAxisOffset(new RectangleInsets(3.0, 3.0, 3.0, 3.0));
		plot.setDomainCrosshairVisible(true);
		plot.setRangeCrosshairVisible(true);

		renderer = new XYLineAndShapeRenderer();
		renderer.setBaseShapesVisible(true);
		renderer.setBaseShapesFilled(true);
		renderer.setBaseStroke(new BasicStroke(2.0f));

		// label the points
		NumberFormat format = NumberFormat.getNumberInstance();
		format.setMaximumFractionDigits(2);
		XYItemLabelGenerator generator = new StandardXYItemLabelGenerator(
				StandardXYItemLabelGenerator.DEFAULT_ITEM_LABEL_FORMAT, format, format);
		renderer.setBaseItemLabelGenerator(generator);
		renderer.setBaseItemLabelsVisible(true);
		plot.setRenderer(renderer);
		ValueAxis axis = plot.getDomainAxis();
		axis.setAutoRange(true);
		axis.setFixedAutoRange(600000.0);
		this.pack();
		RefineryUtilities.centerFrameOnScreen(this);
		this.setVisible(true);

	}

}
