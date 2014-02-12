package display;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.GradientPaint;
import java.util.Map;
import java.util.Map.Entry;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.ui.RefineryUtilities;

/**
 * A real-time bar chart display.
 * 
 * @author abhinav
 * 
 */
public class BarChartDisplay extends GenericChartDisplay {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private DefaultCategoryDataset dataset;
	private String xAxisLabel;
	private String yAxisLabel;

	/**
	 * 
	 * @param title
	 * @param xAxisLabel
	 * @param yAxisLabel
	 * @param imageSaveDirectory
	 * @param dataset
	 */
	public BarChartDisplay(String title, String xAxisLabel, String yAxisLabel,
			String imageSaveDirectory, DefaultCategoryDataset dataset) {
		super(title, imageSaveDirectory);
		this.setDataset(dataset);
		this.xAxisLabel = xAxisLabel;
		this.yAxisLabel = yAxisLabel;

		settings();
		chartPanel = new ChartPanel(getChart());
		chartPanel.setPreferredSize(new Dimension(1024, 600));
		setContentPane(chartPanel);

		chartPanel.setMouseZoomable(true, false);
		pack();
		RefineryUtilities.centerFrameOnScreen(this);

	}

	/**
	 * Continuously refresh values as when the stream output changes to reflect
	 * the same.
	 * 
	 * @param houseId
	 * @param rowValues
	 * @param predictedTime
	 */
	public synchronized void refreshDisplayValues(int houseId, Map<String, Double> rowValues,
			String predictedTime) {

		getChart().setTitle(title + " " + predictedTime);
		for (Entry<String, Double> entry : rowValues.entrySet()) {
			this.getDataset().setValue(entry.getValue(), entry.getKey(), String.valueOf(houseId));
		}

	}

	@Override
	protected void settings() {
		setChart(ChartFactory.createBarChart(title, // chart title
				xAxisLabel, // domain axis label
				yAxisLabel, // range axis label
				getDataset(), // data
				PlotOrientation.VERTICAL, // orientation
				true, // include legend
				true, // tooltips?
				false // URLs?
				));
		getChart().setBackgroundPaint(Color.white);

		// get a reference to the plot for further customisation...
		final CategoryPlot plot = getChart().getCategoryPlot();
		plot.setBackgroundPaint(Color.WHITE);
		plot.setDomainGridlinePaint(Color.LIGHT_GRAY);
		plot.setRangeGridlinePaint(Color.LIGHT_GRAY);

		// set the range axis to display integers only...
		final NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
		rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());

		// disable bar outlines...
		final BarRenderer renderer = (BarRenderer) plot.getRenderer();
		renderer.setDrawBarOutline(false);

		// set up gradient paints for series...
		final GradientPaint gp0 = new GradientPaint(0.0f, 0.0f, Color.blue, 0.0f, 0.0f,
				Color.lightGray);
		final GradientPaint gp1 = new GradientPaint(0.0f, 0.0f, Color.red, 0.0f, 0.0f,
				Color.lightGray);

		renderer.setSeriesPaint(0, gp0);
		renderer.setSeriesPaint(1, gp1);
		renderer.setShadowVisible(false);

		final CategoryAxis domainAxis = plot.getDomainAxis();
		domainAxis.setCategoryLabelPositions(CategoryLabelPositions
				.createUpRotationLabelPositions(Math.PI / 6.0));
		this.setVisible(true);

	}

	public DefaultCategoryDataset getDataset() {
		return dataset;
	}

	public void setDataset(DefaultCategoryDataset dataset) {
		this.dataset = dataset;
	}
}
