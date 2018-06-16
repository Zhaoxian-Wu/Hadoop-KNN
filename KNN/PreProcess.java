package KNN;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.Random;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class PreProcess {
	private static Vector<Float> randomVec;
	private static int vectorSize = 0;
	
	private static final int bucketCount = 10;
	private static float bucketSize;
	
	public static void main(String[] args) throws IOException {
		if(args.length != 2) {
			System.err.println("Usage: hadoop jar PreProcess.jar <dataSet> <bucketSize>");
			System.exit(2);
		}
		
		bucketSize = Float.parseFloat(args[1]);
		
		final String destination = "hdfs://localhost:9000";
		
		Configuration fsconf = new Configuration(); 
		FileSystem fs = FileSystem.get(URI.create(destination), fsconf);

		// 删除旧文件夹
		fs.delete(new Path(destination + "/KNN/DataSetProcess"), true);
		
		// 读入文件
		InputStream inS = fs.open(new Path(args[0]));
		InputStreamReader inSR = new InputStreamReader(inS);
		BufferedReader reader = new BufferedReader(inSR);
		// 输入组
		BufferedWriter[] bw = new BufferedWriter[bucketCount];
		int[] count = new int[bucketCount];
		for(int i=0; i!=bucketCount; ++i) {
			OutputStream os = fs.create(new Path("/KNN/DataSetProcess/" + i));
			OutputStreamWriter osw = new OutputStreamWriter(os);
			bw[i] = new BufferedWriter(osw);
			count[i] = 0;
		}
		// 处理
		String line;
		while((line = reader.readLine()) != null) {
			int bucket = bucketIndex(fs, line);
			bw[bucket].write(line + "\n");
			++count[bucket];
		}
		// 刷新流
		for(int i=0; i!=bucketCount; ++i) {
			bw[i].flush();
			System.out.println("第" + i + "个桶的向量数：" + count[i]);
		}
		
		fs.close();
	}
	
	public static int bucketIndex(FileSystem fs, String vectorStr) throws IllegalArgumentException, IOException {
		// 初始化random
		final String randomFileDest = "/KNN/DataSetProcess/randomVector";
		if(randomVec == null) {
			randomVec = new Vector<Float>();
			String randomStr;
			try { // 已有随机向量
				randomStr = knn.getVectorInFile(fs, randomFileDest);
				for(String s : randomStr.split(", ")) {
					++vectorSize;
					randomVec.add(Float.parseFloat(s));
				}
			} catch (IOException e) { // 未有，生成
				vectorSize = vectorStr.split(", ").length;
				Random random = new Random();
				for(int i=0; i!=vectorSize; ++i) {
					randomVec.add(random.nextFloat());
				}
				String str = randomVec.toString();
				str = str.substring(1, str.length() - 1);
				InputStream in = new ByteArrayInputStream((str.toString() + "\n").getBytes());
				OutputStream out = fs.create(new Path(randomFileDest));
				IOUtils.copyBytes(in, out, 4096, true);
			}
		}
		Vector<Float> vec = getVector(vectorStr);
		
		// H(V) = |V·R + b| / a
		// R是一个随机向量，a是桶宽，b是一个在[0,a]之间均匀分布的随机变量
		int h = 0;
		for(int i=0; i != vectorSize; ++i) {
			h += vec.get(i) * randomVec.get(i);
		}
		h = (int) (Math.abs(h) / bucketSize) % bucketCount;
		return h;
	}
	
	private static Vector<Float> getVector(String str) {
		Vector<Float> vec = new Vector<Float>();
		for(String s : str.split(", ")) {
			vec.add(Float.parseFloat(s));
		}
		return vec;
	}
}
