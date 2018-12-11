package hadoop.mapred.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import hadoop.mapred.mapper.EvenOddMapper;
import hadoop.mapred.reducer.EvenOddReducer;

public class Driver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Path input = new Path("file:////home/rodrir23/evenodd.txt/");
		Path output = new Path("file:////home/rodrir23/output/");
		
		Configuration configuration = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(configuration, "Even Odd Job");
		
		job.setMapperClass(EvenOddMapper.class);
		job.setReducerClass(EvenOddReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		output.getFileSystem(job.getConfiguration()).delete(output, true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
