package test;

public class MatrixLayerTraverse {
	
	
	
	public static void main(String[] args) {
       int [][] a = new int [][]{{1,  2,  3,  4},
    	                         {5,  6,  7,  8},
    	                         {9,  10, 11, 12},
    	                         {13, 14, 15, 16}};
    	                         
    	int m = 4;
    	int n = 4;
    	int i = 0;
    	int j = 0;
    	
    	 
    	
    	while(i<m && j<n && (i>=0 || j>=0)) {
    		System.out.println(a[i][j]);
    	}
       
	}
}
