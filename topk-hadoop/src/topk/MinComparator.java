/**
 * 
 */
package topk;

import java.util.Comparator;

/**
* <p>Title: MinComparator.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016Äê10ÔÂ2ÈÕ
* @version 1.0
*/
public class MinComparator<T> implements Comparator<String> {

	/* (non-Javadoc)
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	@Override
	public int compare(String a, String b) {
		// TODO Auto-generated method stub
		if(b == null)
			return -1;
		if(a == null)
			return 1;
		
		int tmp = a.compareTo(b);
		if(tmp < 0) {
			return 1;
		} else if(tmp == 0) {
			return 0;
		} else {
			return -1;
		}
	}
}
