/**
 * 
 */
package zhongwei.nbc.reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import zhongwei.nbc.writable.MultiWritableComparable;

/**
* <p>Title: Reducer1.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016年10月25日
* @version 1.0
*/
public class Reducer1 extends Reducer<MultiWritableComparable, DoubleWritable, Text, Text> {

	public void reduce(MultiWritableComparable _key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		// process values
		Text[] texts = (Text[]) _key.get().toArray();
		context.write(texts[0], texts[2]);
	}

}
