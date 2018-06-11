package KNN;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KnnMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
	private static Vector<Float> target;
	private int vectorSize = 0;
	
	private static final LongWritable ZERO = new LongWritable(0);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// 初始化target
		if(target == null) {
			target = new Vector<Float>();
			Configuration conf = context.getConfiguration(); 
			String targetStr = conf.get("knn.targetString");
			for(String s : targetStr.split(", ")) {
				++vectorSize;
				target.add(Float.parseFloat(s));
			}
		}
		
		float distSqare = 0;
		String[] vecList = value.toString().split(", ");
		for(int i=0; i!=vectorSize; ++i) {
			float num = Float.parseFloat(vecList[i]);
			float diff = num - target.get(i);
			distSqare += diff * diff;
		}
		float dist = (float)Math.sqrt(distSqare);
		
		String resultStr = "" + dist + "---" + value.toString();
		context.write(ZERO, new Text(resultStr));
	}
}
