package KNN;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KnnCombiner extends Reducer<LongWritable, Text, LongWritable, Text>{
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws NumberFormatException, IOException, InterruptedException {
		Comparator<String> largeFO = new Comparator<String>() {
			@Override
			public int compare(String t1, String t2) {
				float diff = Float.parseFloat(t1.split("---")[0]) 
						- Float.parseFloat(t2.split("---")[0]);
				if(diff > 0) {
					return 1;
				} else if(diff == 0) {
					return 0;
				} else {
					return -1;
				}
			}
		};
		int k = Integer.parseInt(context.getConfiguration().get("knn.k"));
		PriorityQueue<String> queue = new PriorityQueue<String>(k + 1, largeFO);
		
		for(Text value : values) {
			queue.add(value.toString());
		}
		
		for(int i=0; i!=k; ++i) {
			context.write(key, new Text(queue.poll()));
		}
	}
}
