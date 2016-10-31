/**
 * 
 */
package zhongwei.nbc.reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import zhongwei.nbc.writable.MultiWritableComparable;

/**
* <p>Title: Reducer9.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016年10月27日
* @version 1.0
*/
public class Reducer9 extends Reducer<Text, MultiWritableComparable, Text, MultiWritableComparable> {
	
	public void reduce(Text _key, Iterable<MultiWritableComparable> values, Context context) throws IOException, InterruptedException {
		// process values
		Map<String, String> map = new HashMap<String, String>();
		for (MultiWritableComparable val : values) {
			Text[] array = (Text[]) val.get().get();
			if(array.length == 3) {
				map.put(array[0].toString(), array[1].toString());
			}
		}
		
		for (MultiWritableComparable val : values) {
			Text[] array = (Text[]) val.get().get();
			if(array.length == 2) {
				for(Map.Entry<String, String> entry : map.entrySet()) {
					List<String> list = new LinkedList<String>();
					list.add(_key.toString());
					list.add(entry.getKey());
					list.add(entry.getValue());
					list.add(array[1].toString());
					context.write(new Text(array[0]), new MultiWritableComparable(list));
				}
			}
		}
	}

}
