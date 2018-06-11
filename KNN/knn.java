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
		
		if(args.length != 3) {
			System.err.println("Usage: KNN <targetFile> <DataSet> <k of KNN>");
			System.exit(2);
		}
		
		String destination = "hdfs://localhost:9000";

		// 读入target
		Configuration conf = new Configuration(); 
		FileSystem fs = FileSystem.get(URI.create(destination), conf);
		InputStream in = fs.open(new Path(args[0]));
		OutputStream targetStrem = new ByteArrayOutputStream();
		IOUtils.copyBytes(in, targetStrem, 4096, false);
		// 删除旧文件夹
		fs.delete(new Path(destination + "/KNN/result"), true);
		fs.close();
		
		// 执行MapReduce
		// Configuration conf = new Configuration(); // 上面已经有Configuration
		conf.set("knn.targetString", targetStrem.toString());
		conf.set("knn.k", args[2]);
		
		Job job = Job.getInstance(conf, "KNN");
		
		job.setJarByClass(knn.class);
		job.setMapperClass(KnnMapper.class);
		job.setCombinerClass(KnnCombiner.class);
		job.setReducerClass(KnnReducer.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path("/KNN/result"));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
