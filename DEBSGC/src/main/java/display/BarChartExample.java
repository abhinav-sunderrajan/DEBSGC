package display;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;

import org.jfree.data.category.DefaultCategoryDataset;

public class BarChartExample {

	private static BarChartDisplay valuesOutput;
	private static HashMap<String, Double> barchartDisplayMap;
	public final static String CURRENT_LOAD = "Current Load";
	public final static String PREDICTED_LOAD = "Predicted Load";

	public static void main(String[] args) {
		valuesOutput = new BarChartDisplay("Load Prediction", "House ID", "Load(W)",
				"/home/abhinav/Desktop/", new DefaultCategoryDataset());
		barchartDisplayMap = new HashMap<String, Double>();
		barchartDisplayMap.put(CURRENT_LOAD, 0.0);
		barchartDisplayMap.put(PREDICTED_LOAD, 0.0);

		for (short i = 0; i < 40; i++) {
			for (Entry<String, Double> entry : barchartDisplayMap.entrySet()) {
				valuesOutput.getDataset().addValue(entry.getValue(), entry.getKey(),
						String.valueOf(i));
				valuesOutput.getDataset().addValue(entry.getValue(), entry.getKey(),
						String.valueOf(i));
			}

		}

		Random rand = new Random();
		for (int i = 0; i < 1000000; i++) {
			barchartDisplayMap.put(CURRENT_LOAD, rand.nextDouble() * 100);
			barchartDisplayMap.put(PREDICTED_LOAD, rand.nextDouble() * 100);
			valuesOutput.refreshDisplayValues(rand.nextInt(40), barchartDisplayMap,
					String.valueOf(rand.nextGaussian() * Math.PI));

			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}
}
