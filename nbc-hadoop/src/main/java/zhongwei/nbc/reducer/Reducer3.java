/**
 * 
 */
package zhongwei.nbc.reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import zhongwei.nbc.writable.MultiWritableComparable;

/**
* <p>Title: Reducer3.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016年10月25日
* @version 1.0
*/
public class Reducer3 extends Reducer<MultiWritableComparable, DoubleWritable, MultiWritableComparable, MultiWritableComparable> {

	public void reduce(MultiWritableComparable _key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		// process values
		int count = 0;
		for (DoubleWritable val : values) {
			count++;
		}

		Text[] key = (Text[])_key.get().toArray();
		List<String> listKey = new LinkedList<String>();
		listKey.add(key[1].toString());
		listKey.add(key[2].toString());
		
		List<String> listValue = new LinkedList<String>();
		listValue.add(key[0].toString());
		listValue.add(String.valueOf(count));
		
		context.write(new MultiWritableComparable(listKey), new MultiWritableComparable(listValue));
	}
}
