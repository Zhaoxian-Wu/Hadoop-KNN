package KNN;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class knn {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		if(args.length != 2) {
			System.err.println("Usage: hadoop jar KNN.jar <targetFile> <k of KNN>");
			System.exit(2);
		}
		
		String destination = "hdfs://localhost:9000";

		Configuration fsconf = new Configuration(); 
		FileSystem fs = FileSystem.get(URI.create(destination), fsconf);

		// 读入target
		String target = getVectorInFile(fs, args[0]);
		// 找到target的桶
		int bucket = PreProcess.bucketIndex(fs, target);
		
		// 删除旧文件夹
		fs.delete(new Path(destination + "/KNN/result"), true);
		fs.close();
		
		// 执行MapReduce
		Configuration conf = new Configuration();
		conf.set("knn.targetString", target);
		conf.set("knn.k", args[1]);
		
		Job job = Job.getInstance(conf, "KNN");
		
		// 指定相关类
		job.setJarByClass(knn.class);
		job.setMapperClass(KnnMapper.class);
		job.setCombinerClass(KnnCombiner.class);
		job.setReducerClass(KnnReducer.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		// 设置运行Reduce的节点个数
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path("/KNN/DataSetProcess/" + bucket));
		FileOutputFormat.setOutputPath(job, new Path("/KNN/result"));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static String getVectorInFile(FileSystem fs, String file) throws IOException { 
		InputStream in = fs.open(new Path(file));
		OutputStream targetStream = new ByteArrayOutputStream();
		IOUtils.copyBytes(in, targetStream, 4096, false);
		return targetStream.toString();
	}
}
