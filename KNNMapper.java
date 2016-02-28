import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class KNNMapper extends Mapper<LongWritable, Text, Text, Text> {
	public static final int K = 3;
	private Text label = new Text();
	private List<int[]> trainingdata;
	private List<String> trainingclass;

	private class Item {
		private String label;
		private Double distancevalue;

		public Item(String key, double value) {
			label = key;
			distancevalue = value;
		}

	}

	protected void setup(Context context) {
		trainingdata = new ArrayList<int[]>();
		trainingclass = new ArrayList<String>();
		try {
			String line;
			FileInputStream fileinput = new FileInputStream(new File("/Users/ouxiaocheng/Documents/iris_train.csv"));
            BufferedReader reader = new BufferedReader(new InputStreamReader(fileinput));
			while ((line = reader.readLine()) != null){
				String[] a = line.split(",");
				int[] b = new int[a.length];
				for(int i=0;i<a.length-1;i++){
					b[i] = Integer.parseInt(a[i]);
					
				}
				trainingdata.add(b);
				trainingclass.add(a[a.length-1]);
			}
			reader.close();
		}catch (Exception e) {

		}
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		Queue<Item> queue = new PriorityQueue<Item>(K, new Comparator<Item>(){
            public int compare(Item c1, Item c2) {
      			 return c2.distancevalue.compareTo(c1.distancevalue);
    		 }

		});
		int i=0;
		String[] testline = line.split(",");
		int[] btest = new int[testline.length];
		for(int j=0;j<testline.length-1;j++){
			btest[j] = Integer.parseInt(testline[j]);
		}
		for(int[] trainline : this.trainingdata){
            double distance = euclideanDistance(trainline, btest );
            queue.add(new Item(this.trainingclass.get(i++),distance));
            if (queue.size() > K)
				queue.poll();
		}
		for (Item queueitem : queue){
			label.set(queueitem.label);
			context.write(value, label);
		}
	}



	private double euclideanDistance(int trainline[], int testline[]) {
		double sum = 0.0;
		for (int i = 0; i <= 3; i++){
			sum += Math.pow(trainline[i] - testline[i], 2);
			
		}
		return Math.sqrt(sum);
	}
}