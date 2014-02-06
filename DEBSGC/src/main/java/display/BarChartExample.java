package display;

import java.util.Random;

import org.jfree.data.category.DefaultCategoryDataset;

public class BarChartExample {

	private static BarChartDisplay valuesOutput;

	public static void main(String[] args) {
		valuesOutput = new BarChartDisplay("Load Prediction", "/home/abhinav/Desktop/",
				new DefaultCategoryDataset());

		Random rand = new Random();
		for (int i = 0; i < 1000000; i++) {
			valuesOutput.refreshDisplayValues(rand.nextInt(40), rand.nextDouble() * 100,
					rand.nextDouble() * 100, String.valueOf(rand.nextGaussian() * Math.PI));

			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}
}
