package word.Cooccurrence;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StripesReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		HashMap<String, Integer> wordCooccurrence = new HashMap<String, Integer>();
		int count = 1;
		for (Text neighbor : value) {
			if (wordCooccurrence.containsKey(neighbor.toString())) {
				count = wordCooccurrence.get(neighbor.toString())+1;
			}
			wordCooccurrence.put(neighbor.toString(), count);
		}
		context.write(key, new Text(wordCooccurrence.toString()));

	}
}
