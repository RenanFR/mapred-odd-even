package hadoop.mapred.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EvenOddReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	protected void reduce(Text text, Iterable<IntWritable> iterable,
			Context context) throws IOException, InterruptedException {
		int sumEven = 0;
		int sumOdd = 0;
		for (@SuppressWarnings("unused") IntWritable number : iterable) {
			if (text.toString().equals("EVEN")) {
				sumEven += 1;
			} else {
				sumOdd += 1;
			}
		}
		if (text.toString().equals("EVEN")) {
			context.write(new Text("SUM EVEN"), new IntWritable(sumEven));
		} else {
			context.write(new Text("SUM ODD"), new IntWritable(sumOdd));
		}
	}
}
