package word.Cooccurrence;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StripesMapper extends Mapper<Object, Text, Text, Text> {
	Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\\s+");
		if (tokens.length > 1) {
			for (int i = 0; i < tokens.length; i++) {
				word.set(tokens[i]);
				int neighborsLength = 2;
				int start = (i - neighborsLength) < 0 ? 0 : i - neighborsLength;
				int end = (neighborsLength + i) >= tokens.length ? tokens.length - 1 : neighborsLength + i;
				for (int j = start; j <= end; j++) {
					if (j == i)
						continue;
					Text neighbor=new Text(tokens[j]);
					context.write(word, neighbor);
				}

			}

		}
	}

}
