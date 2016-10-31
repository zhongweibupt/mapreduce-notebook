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

import zhongwei.nbc.utils.Util;
import zhongwei.nbc.writable.MultiWritableComparable;

/**
* <p>Title: Reducer10.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016年10月27日
* @version 1.0
*/
public class Reducer10 extends Reducer<Text, MultiWritableComparable, MultiWritableComparable, DoubleWritable> {

	public void reduce(Text _key, Iterable<MultiWritableComparable> values, Context context) throws IOException, InterruptedException {
		// process values
		double sumOfAllIdf = 0.0;
		
		for (MultiWritableComparable val : values) {
			Text[] array = (Text[]) val.get().get();
			if(array.length == 1) {
				sumOfAllIdf = Double.valueOf(array[0].toString());
				break;
			}
		}
		
		for (MultiWritableComparable val : values) {
			Text[] array = (Text[]) val.get().get();
			if(array.length != 1) {
				String word = array[0].toString();
				String id = array[1].toString();
				int count = Integer.valueOf(array[2].toString());
				double sumOfIdf = Double.valueOf(array[3].toString());
				
				double prob = Math.log(
						(sumOfIdf + Util.LAPLACE_SMOOTH_PARA) /
						(sumOfAllIdf + Util.LAPLACE_SMOOTH_PARA * Util.SIZE_WORDTABLE));
				
				List<String> list = new LinkedList<String>();
				list.add(id);
				list.add(_key.toString());
				
				for(int i = 0; i < count; i++) {
					context.write(new MultiWritableComparable(list), new DoubleWritable(prob));
				}
			}
		}
	}

}
