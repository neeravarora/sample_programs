package com.raj;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Anagram {
	
		  static void initiate(String s)
		  {
//		    permute("", s);
//		    System.out.println("----------------------------------------- ");
//		    combo("", s);

//		    System.out.println("----------------------------------------- ");
			  
			  
			  permuteWithRepeative("", "baaba");
		   
//			    System.out.println("----------------------------------------- ");
			  //pattern("", "01", 3);
//			  System.out.println("----------------------------------------- ");
//			  Set<Character> contraintSet = new HashSet<>();
//			  contraintSet.add('0');
//			  contraintSet.add('1');
//			  patternWithoutAdjacentValues("", "012", 3, 'n', contraintSet);
//			  
//			  System.out.println("----------------------------------------- ");
//			  contraintSet.clear();
//			  contraintSet.add('a');
//			  contraintSet.add('b');
//			  contraintSet.add('c');
//			  patternWithoutAdjacentValues("", "abc", 2, 'n', contraintSet);
		  }

		  static void combo(String prefix, String s)
		  {
		    int N = s.length();

		    System.out.println(prefix);

		    for (int i = 0 ; i < N ; i++)
		      combo(prefix + s.charAt(i), s.substring(i+1));
		  }
		  static void permute(String prefix, String s)
		  {
		    int N = s.length();

		    if (N == 0)
		      System.out.println(" " + prefix);

		    for (int i = 0 ; i < N ; i++)
		      permute(prefix + s.charAt(i), s.substring(0, i) + s.substring(i+1, N));
		  }
		  
	static void permuteWithRepeative(String prefix, String s) {
		int N = s.length();

		if (N == 0)
			System.out.println(" " + prefix);
		Set<Character> charSet = new HashSet<>();
		for (int i = 0; i < N; i++)
			if (!charSet.contains(s.charAt(i))) {
				charSet.add(s.charAt(i));
				permuteWithRepeative(prefix + s.charAt(i), s.substring(0, i) + s.substring(i + 1, N));
			}
	}

		  static void pattern(String prefix, String tokens, int r)
		  {
		    int N = tokens.length();

		    if (r == 0)
		      System.out.println(" " + prefix);
		    else
		    for (int i = 0 ; i < N ; i++)
		    	pattern(prefix + tokens.charAt(i), tokens, r-1);
		  }
		  
		  
	static void patternWithoutAdjacentValues(String prefix, String tokens, int r, char lastPrinted,
			Set<Character> withoutAdjStr) {
		int N = tokens.length();
		if (r == 0)
			System.out.println(" " + prefix);
		else
			for (int i = 0; i < N; i++) {
				if (withoutAdjStr.contains(lastPrinted) && lastPrinted == tokens.charAt(i))
					continue;

				patternWithoutAdjacentValues(prefix + tokens.charAt(i), tokens, r - 1, tokens.charAt(i), withoutAdjStr);
			}
	}
	


		
	
	
	
	
	
	
	
	public static void main(String[] args) {
//       String input = "abc";
//       StringBuilder sb = new StringBuilder(input);
//       //sb.insert(1, "ef");
//       //System.out.println(sb);
//      // sb.delete(1, 3);
//      // System.out.println(sb);
//       anagram(sb, new StringBuilder(), 0);
       
       String s = "abc";
	    initiate(s);
       
	}

	private static void anagram(StringBuilder input, StringBuilder output, int count) {
		if(output.length() == 3) {
			//System.out.println(output);
			return;
		}
		
		for (int i = count; i < input.length(); i++) {
			char c = input.charAt(i);
			output.insert(count, c);
			input = input.deleteCharAt(i);
			System.out.println(input+", "+c+", "+output);
			anagram(input, output, ++count);
			input = input.insert(i, c);
			System.out.println("----> "+input+", "+c+", "+output);
			
		}
		
		
	}
	
	static class My implements Comparator<List<Integer>>{

		@Override
		public int compare(List<Integer> arg0, List<Integer> arg1) {
			// TODO Auto-generated method stub
			return 0;
		}}
}
