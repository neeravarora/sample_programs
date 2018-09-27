package test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class Laxigraphy {
	
	
	
	public static void main(String[] args) {
		List<String> list = new ArrayList();
		list.add("abc");
		list.add("def");
		//list.add("pqrs");
		getPatt(list);
	}
	
//	static List<String> getPatt(List<String> l){
//		char[] path = new char[l.size()];
//		for (int i = 0; i < l.size(); i++) {
//			char[] a = l.get(i).toCharArray();
//			for (int j = 0; j < a.length; j++) {
//				path[i] = a[j];
//				//System.out.print(a[j]);
//			}
//			System.out.println();
//		}
//		
//		return null;
//	}
	
	static List<String> getPatt(List<String> l) {
		Queue<String> queue = new LinkedList();
		queue.add("");
		for (int i = 0; i < l.size(); i++) {
			char[] a = l.get(i).toCharArray();

			System.out.println(l.get(i));
			while (queue.element().length()<i+1) {
				String s = queue.remove();
				for (int j = 0; j < a.length; j++) {
					// path[i] = a[j];
					// System.out.print(a[j]);
					queue.add(s + a[j]);
					System.out.println(s + a[j]);
				}
			}
		}
		System.out.println();
		System.out.println(queue);
		return null;
	}
	
	
	static List<String> getPatt2(List<String> l) {
		StringBuilder sb = new StringBuilder();
		int i = 0;
		int j = 0;
		int s = l.size();
		while (i < l.size() && j < l.get(i).length()) {
			while (j < l.get(i).length()) {
				sb.setCharAt(j, l.get(i).charAt(j));
				j++;
				if(j >= l.get(i).length()) {
				i++;
				j--;
				if(i >= s)
					i=0;
				}
			}
			
		}
		return null;
	}

}
