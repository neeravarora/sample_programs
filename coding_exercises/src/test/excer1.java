package test;

public class excer1 {

	public static void main(String[] args) {

		int[] arr = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,  11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21  };

		
		// // Zig zag normal fashion
//		int level = 1;
//		for (int i = 0; i < arr.length; ) {
//			int noOfSib = level;
//			while(noOfSib > 0){
//				System.out.print(arr[i] + ", ");
//				noOfSib--;
//				i++;
//			}
//			level++;
//			System.out.print("\n");
//		}
		
		// Zig zag fashion
		
		int level = 1;
		
		for (int i = 0; i < arr.length;) {
			int noOfSib = level;
			if (level % 2 == 0) {
				int endOfIndexForLevel = noOfSib + i -1;
				while (noOfSib > 0) {
					System.out.print(arr[endOfIndexForLevel--] + ", ");
					noOfSib--;
					i++;
				}
			} else
				while (noOfSib > 0) {
					System.out.print(arr[i++] + ", ");
					noOfSib--;
				}
			level++;
			System.out.print("\n");
		}

	}

}
