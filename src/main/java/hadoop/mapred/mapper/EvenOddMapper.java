package hadoop.mapred.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//	TextInputFormat
public class EvenOddMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] numbersStr = value.toString().split(",");
		Long[] numbers = new Long[numbersStr.length];
		for (int i = 0; i < numbersStr.length; i++) {
			numbers[i] = Long.valueOf(numbersStr[i]);
		}
		for (Long n : numbers) {
			if (n % 2 == 0) {
				context.write(new Text("EVEN"), new IntWritable(n.intValue()));
			} else {
				context.write(new Text("ODD"), new IntWritable(n.intValue()));
			}
		}
	}
	
}
