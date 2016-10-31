/**
 * 
 */
package zhongwei.nbc.reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import zhongwei.nbc.writable.MultiWritableComparable;

/**
* <p>Title: Reducer11.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016年10月27日
* @version 1.0
*/
public class Reducer11 extends Reducer<MultiWritableComparable, DoubleWritable, MultiWritableComparable, DoubleWritable> {

	public void reduce(MultiWritableComparable _key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		// process values
		double sum = 0.0;
		for (DoubleWritable val : values) {
			sum += val.get();
		}
		context.write(_key, new DoubleWritable(sum));
	}

}
