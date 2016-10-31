/**
 * 
 */
package zhongwei.nbc.reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
* <p>Title: Reducer7.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016年10月27日
* @version 1.0
*/
public class Reducer7 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text _key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		// process values
		double sum = 0.0;
		for (DoubleWritable val : values) {
			sum += val.get();
		}
		context.write(_key, new DoubleWritable(sum));
	}
}
