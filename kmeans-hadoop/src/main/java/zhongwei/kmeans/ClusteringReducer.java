/**
 * 
 */
package zhongwei.kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
* <p>Title: ClusteringReducer.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016年10月9日
* @version 1.0
*/
public class ClusteringReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values, key - cluster_id, values - data
		if(!values.iterator().hasNext()) {
			//throw new IllegalStateException("Size of members in " + id + " is empty.");  
			//center = KMeans.chooseCenter(1);
			//return;
		}
		
		Configuration conf = context.getConfiguration();
		
		StringBuffer buf = new StringBuffer();
		List<Double> data = new ArrayList<Double>();
		
		List<String[]> strList = new LinkedList<String[]>();
		for(Text val : values) {
			strList.add(val.toString().split(","));
		}
		for (int i = 0; i < Integer.valueOf(conf.get("kmeans.dimension")); i++) {  
            double tmp = 0.0;
            int count = 0;
			for(int j = 0; j < strList.size(); j++) {
				String str[] = strList.get(j);
            	tmp += Double.valueOf(str[i]);
            	count++;
            }
			tmp /= count;
			data.add(tmp);
        }  
		
        Text center = new Text(new Point(_key.toString(), data).getDataString());
        context.write(_key, center);
	}
}
