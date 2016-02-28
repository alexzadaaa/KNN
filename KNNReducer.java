import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class KNNReducer extends Reducer<Text, Text, Text, Text> {
	private Text result = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int globalmax = 0;
		String labels = null;
		Map<String, Integer> map = new HashMap<String, Integer>();
		for (Text label : values) {
			labels =label.toString();
			if (map.containsKey(labels)){
				map.put(labels,map.get(labels)+1);
			}else{
				map.put(labels,1);
			}

		}
		for (Map.Entry<String, Integer> label : map.entrySet())
			if (label.getValue() > globalmax) {
				labels = label.getKey();
				globalmax = label.getValue();
			}
		result.set(labels);
		context.write(key, result);
	}
}