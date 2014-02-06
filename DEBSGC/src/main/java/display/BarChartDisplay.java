package display;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.GradientPaint;

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
	private final String CURRENT_LOAD = "Current Load";
	private final String PREDICTED_LOAD = "Predicted Load";

	public BarChartDisplay(String title, String imageSaveDirectory, DefaultCategoryDataset dataset) {
		super(title, imageSaveDirectory);
		this.dataset = dataset;
		for (short i = 0; i < 40; i++) {
			this.dataset.addValue(0.0, CURRENT_LOAD, String.valueOf(i));
			this.dataset.addValue(0.0, PREDICTED_LOAD, String.valueOf(i));
		}

		settings();
		chartPanel = new ChartPanel(chart);
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
	 * @param currentAvg
	 * @param predictedLoad
	 * @param predictedTime
	 */
	public synchronized void refreshDisplayValues(int houseId, double currentAvg,
			double predictedLoad, String predictedTime) {

		chart.setTitle(title + " " + predictedTime);
		this.dataset.setValue(currentAvg, CURRENT_LOAD, String.valueOf(houseId));
		this.dataset.setValue(predictedLoad, PREDICTED_LOAD, String.valueOf(houseId));

	}

	@Override
	protected void settings() {
		chart = ChartFactory.createBarChart(title, // chart title
				"House ID", // domain axis label
				"Load(W)", // range axis label
				dataset, // data
				PlotOrientation.VERTICAL, // orientation
				true, // include legend
				true, // tooltips?
				false // URLs?
				);
		chart.setBackgroundPaint(Color.white);

		// get a reference to the plot for further customisation...
		final CategoryPlot plot = chart.getCategoryPlot();
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

		final CategoryAxis domainAxis = plot.getDomainAxis();
		domainAxis.setCategoryLabelPositions(CategoryLabelPositions
				.createUpRotationLabelPositions(Math.PI / 6.0));
		this.setVisible(true);

	}
}
