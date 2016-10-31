/**
 * 
 */
package zhongwei.nbc.reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import zhongwei.nbc.writable.MultiWritableComparable;

/**
* <p>Title: Reducer8.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016年10月27日
* @version 1.0
*/
public class Reducer8 extends Reducer<MultiWritableComparable, DoubleWritable, Text, MultiWritableComparable> {

	public void reduce(MultiWritableComparable _key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		// process values
		double count = 0.0;
		for (DoubleWritable val : values) {
			count += val.get();
		}
		
		Text[] texts = (Text[]) _key.get().get();
		String word = texts[0].toString();
		String id = texts[1].toString();
		
		List<String> list = new LinkedList<String>();
		list.add(id);
		list.add(String.valueOf(count));
		list.add(texts[2].toString());
		
		context.write(new Text(word), new MultiWritableComparable(list));
	}
}
