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
* <p>Title: Reducer5.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016年10月27日
* @version 1.0
*/
public class Reducer5 extends Reducer<Text, MultiWritableComparable, MultiWritableComparable, DoubleWritable> {

	public void reduce(Text _key, Iterable<MultiWritableComparable> values, Context context) throws IOException, InterruptedException {
		// process values
		int numContainsWord = -1;
		for (MultiWritableComparable val : values) {
			Text[] texts = (Text[]) val.get().toArray();
			if(texts.length == 1) {
				numContainsWord = Integer.valueOf(texts[0].toString());
			}
		}
		
		double idf = 0.0;
		String label = new String();
		double count = 0.0;
		for(MultiWritableComparable val : values) {
			Text[] texts = (Text[]) val.get().toArray();
			if(texts.length == 2) {
				label = texts[0].toString();
				count = Double.valueOf(texts[1].toString());
			}
			idf = count * Math.log((double) Util.SIZE_ARTICLES/numContainsWord);
			
			List<String> list = new LinkedList<String>();
			list.add(label);
			list.add(_key.toString());
			context.write(new MultiWritableComparable(list), new DoubleWritable(idf));
		}
	}
}
