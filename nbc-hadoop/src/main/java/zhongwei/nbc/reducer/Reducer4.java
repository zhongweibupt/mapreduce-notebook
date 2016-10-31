/**
 * 
 */
package zhongwei.nbc.reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import zhongwei.nbc.utils.Util;
import zhongwei.nbc.writable.MultiWritableComparable;

/**
* <p>Title: Reducer4.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016年10月27日
* @version 1.0
*/
public class Reducer4 extends Reducer<MultiWritableComparable, MultiWritableComparable, MultiWritableComparable, DoubleWritable> {

	public void reduce(MultiWritableComparable _key, Iterable<MultiWritableComparable> values, Context context) throws IOException, InterruptedException {
		// process values
		Text[] key = (Text[]) _key.get().get();
		String label = key[0].toString();
		
		Map<String, Integer> counts = new HashMap<String, Integer>();
		for (MultiWritableComparable val : values) {
			Text[] array = (Text[]) val.get().get();
			counts.put(array[0].toString(), Integer.valueOf(array[1].toString()));
		}
		
		int num = counts.size();
		for(Map.Entry<String, Integer> entry : counts.entrySet()) {
			double tfNormalize = (double)entry.getValue()/(num + 1);
			double result = tfNormalize;// * Math.log((double)Util.SIZE_ARTICLES/Util.queryArticlesNumContainsWord(entry.getKey()));
			
			List<String> list = new LinkedList<String>();
			list.add(label);
			list.add(entry.getKey());
			context.write(new MultiWritableComparable(list), new DoubleWritable(result));
		}
	}
}
