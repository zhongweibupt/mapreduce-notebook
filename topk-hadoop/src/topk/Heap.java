/**
 * 
 */
package topk;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/**
* <p>Title: Heap.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016Äê10ÔÂ1ÈÕ
* @version 1.0
*/

public class Heap<T> {
	private int k;
	private List<T> data;
	private Comparator<T> comparator;
	
	public Heap(int k, Comparator<T> comparator) {
		this.k = k;
		this.data = new ArrayList<T>();
		for(int i = 0; i < k; i++) {
			this.data.add(null);
		}
		this.comparator = comparator;
	}
	
	protected void adjustHeap(int i, Comparator<T> comparator) {
		int left = left(i);
		int right = right(i);
		
		int tmp = i;
		
		if(left < k 
				&& comparator.compare(data.get(left), data.get(tmp)) > 0) {
			tmp = left;
		}
		
		if(right < k 
				&& comparator.compare(data.get(right), data.get(tmp)) > 0) {
			tmp = right;
		}
		
		if(i == tmp)
			return;
		
		swap(i, tmp);
		
		adjustHeap(tmp, comparator);
	}
	
	private int right(int i) {
		return (i + 1) << 1;
	}
	
	private int left(int i) {
		return ((i + 1) << 1) - 1;
	}
	
	private void swap(int i, int j) {
		T tmp = data.get(i);
		data.set(i, data.get(j));
		data.set(j, tmp);
	}
	
	public int getK() {
		return this.k;
	}
	
	public List<T> getData() {
		return new LinkedList<T>(this.data);
	}
	
	public void add(T element) {
		if(comparator.compare(data.get(0), element) > 0) {
			data.set(0, element);
			adjustHeap(0, comparator);
		}
	}
	
	/*
	public static void main(String args[]) {
		int[] a = {12,1,5425,12,43,734,343,999,8,99,55,2,222,444,11};
		
		HeapComparator<Integer> comparator = new HeapComparator<Integer>();
		Heap<Integer> heap = new Heap<Integer>(10, comparator);
		for(int i : a) {
			heap.add(i);
			System.out.println(heap.getData());
		}
		
		System.out.println(heap.getData());
	}*/
}
