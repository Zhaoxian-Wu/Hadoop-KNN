package KNN;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.URI;
import java.util.Random;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class KnnSampleGenarator {

	public static void main(String[] args) 
			throws IllegalArgumentException, IOException {
		if(args.length != 3) {
			System.err.println("Usage: KnnSampleGenarator <SampleSize> <VectorSize> <K of KNN>");
			System.exit(2);
		}
		
		
		int sampleSize = Integer.parseInt(args[0]);
		int vectorSize = Integer.parseInt(args[1]);
		int k = Integer.parseInt(args[2]);
		
		Random random = new Random();
		// 目标向量
		Vector<Float> target = new Vector<Float>(vectorSize);
		for(int i=0; i!=vectorSize; ++i) {
			target.add(random.nextFloat());
		}
		String targetStr = target.toString();
		targetStr = targetStr.substring(1, targetStr.length() - 1);
		
		// 创建（sampleSize - k / 2）个远邻（每个分量随机是目标向量的3-5倍）
		StringWriter swriter = new StringWriter();
		for(int i=0; i != sampleSize - k / 2; ++i) {
			// 创建向量
			Vector<Float> vec = new Vector<Float>(vectorSize);
			for(int j=0; j != vectorSize; ++j) {
				vec.add(target.get(j) * (Math.abs(random.nextInt()) % 5 + 3));
			}
			String vecStr = vec.toString();
			vecStr = vecStr.substring(1, vecStr.length() - 1) + "\n";
			swriter.write(vecStr);
		}
		// 创建（k / 2）个近邻（每个分量随机偏离目标向量分量0-10%）
		for(int i=0; i != k / 2; ++i) {
			// 创建向量
			Vector<Float> vec = new Vector<Float>(vectorSize);
			for(int j=0; j != vectorSize; ++j) {
				vec.add(target.get(j) * (1 + random.nextFloat() * 0.1f));
			}
			String vecStr = vec.toString();
			vecStr = vecStr.substring(1, vecStr.length() - 1) + "\n";
			swriter.write(vecStr);
		}

		String destination = "hdfs://localhost:9000/KNN/sample";
		Configuration conf = new Configuration(); 
		FileSystem fs = FileSystem.get(URI.create(destination), conf);

		// 写入数据集
		{
			InputStream in = new ByteArrayInputStream(swriter.toString().getBytes());
			OutputStream out = fs.create(new Path(destination + "/dataSet"));
			IOUtils.copyBytes(in, out, 4096, true);
		}
		// 写入目标向量
		{
			String str = target.toString();
			str = str.substring(1, str.length() - 1);
			InputStream in = new ByteArrayInputStream(str.toString().getBytes());
			OutputStream out = fs.create(new Path(destination + "/target"));
			IOUtils.copyBytes(in, out, 4096, true);
		}
	}

}
