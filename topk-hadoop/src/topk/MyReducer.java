/**
 * 
 */
package topk;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
* <p>Title: MyReducer.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016Äê10ÔÂ2ÈÕ
* @version 1.0
*/
public class MyReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
	public static final int K = 10;
	private Heap<String> heap = new Heap<String>(10, new MinComparator<String>());

	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values
		for (Text val : values) {
			heap.add(val.toString());
		}
		
		Text text = new Text();
		for(String str : heap.getData()) {
			text.set(str);
			context.write(NullWritable.get(), text);
		}
	}
}
