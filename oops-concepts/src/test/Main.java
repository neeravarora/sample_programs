package test;

import java.util.regex.Pattern;

public class Main {
	public static void main(String[] args) {
     //String regex = "[^//?=,\\(\\)\\[\\]]+$";

     String ALL_VALID_CHAR = "[0-9A-Za-z #$%=@!{},`~&*()'<>.:;_|^\\/+\\[\\]\"\\\\-]{1,100}";
     Pattern validCharRegex = Pattern.compile(ALL_VALID_CHAR);
    // System.out.println("=abh?hpp".matches(regex));
     
     
     String s1 = "!as`!-|_+=@#$%^&(*)()- _+=-\\\\_+={}{}[]ddds2!@@#@$@@$@$$354%^|A453k5<>.,:;:'\"|//\\\\***EWEYT54465";
     System.out.println(s1.length());
	System.out.println(s1.matches(ALL_VALID_CHAR));
     System.out.println(validCharRegex.matcher(s1).matches());
     
     System.out.println("/\\".matches(ALL_VALID_CHAR));
     System.out.println("AAaabc123".matches(ALL_VALID_CHAR));
     
     //String regex =  "^1(01)+";
    // (00|11|(01|10)(00|11)*(01|10))*""
     String regex =  "^1((00)|(010)|(100)|(001))((00)*1*|(010)*1*|(100)*1*|(001)*1*)";
     String s2 = "1001001110";
     System.out.println(s2.matches(regex));
  
 	
	}
	
	static class Output{
        private int noOfReq;
        private Long sizeOfData;
		public int getNoOfReq() {
			return noOfReq;
		}
		public void setNoOfReq(int noOfReq) {
			this.noOfReq = noOfReq;
		}
		public Long getSizeOfData() {
			return sizeOfData;
		}
		public void setSizeOfData(Long sizeOfData) {
			this.sizeOfData = sizeOfData;
		}
        
        
    }
}
