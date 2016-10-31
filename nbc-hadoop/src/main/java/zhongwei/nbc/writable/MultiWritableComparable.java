/**
 * 
 */
package zhongwei.nbc.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
/**
* <p>Title: MultiWritableComparable.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016年10月24日
* @version 1.0
*/
public class MultiWritableComparable implements WritableComparable<MultiWritableComparable> {

	private ArrayWritable array;
	
	public MultiWritableComparable() {
		set(new ArrayWritable(Writable.class));
	}
	
	public MultiWritableComparable(ArrayWritable array) {
		set(array);
	}
	
	public MultiWritableComparable(List<String> list) {
		Text[] values = new Text[list.size()];
		
		int i = 0;
		for(String str : list) {
			values[i++] = new Text(str);
		}
		array.set(values);
		set(array);
	}
	
	public void set(ArrayWritable array) {
		this.array = array;
	}
	
	public ArrayWritable get() {
		return array;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.array.readFields(arg0);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.array.write(arg0);
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(MultiWritableComparable o) {
		// TODO Auto-generated method stub
		Text[] o1 = (Text[])this.array.get();
		Text[] o2 = (Text[])o.get().get();
		
		for(int i = 0; i < o1.length && i < o2.length; i++) {
			int cmp = o1[i].compareTo(o2[2]);
			if(cmp != 0) {
				return cmp;
			}
		}
		
		if(o1.length == o2.length) {
			return 0;
		}
		
		return o1.length > o2.length ? 1 : -1;
	}
	
	@Override
	public boolean equals(Object o) {
		if(o instanceof MultiWritableComparable) {
			MultiWritableComparable mc = (MultiWritableComparable) o;
			return mc.get().equals(this.array);
		}
		
		return false;
	}
	
	@Override
	public int hashCode() {
		return this.array.hashCode();
	}

}
