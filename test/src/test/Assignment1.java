package test;

public class Assignment1 {
//	static int N= 8, r1=1, r2=2;
//	static String S= "########";
	
	
	static int N= 20, r1=1, r2=2;
	static String S= "####################";
	
//	static int N= 5, r1=1, r2=5;
//	static String S= "#***#";
	
	
	
	static final char trap = '*';
	static final char safe = '#';
	static final String noWay = "No Way";
	
	public static void main(String[] args) {
		//test();
		solve();	
	}

	private static void solve() {
		int jump = getJumpCount(r1, r2, N, S);
		if(jump > N)
			System.out.println(noWay);
		else
		   System.out.println(jump);
	}
	
	static int getJumpCount(int r1, int r2, int N, String S) {
		char[] cells = S.toCharArray();
		if (cells[0] == trap)
			return N + 1;
		
		int[] jumpCount = getJumpCntArray2(r1, r2, N, cells);

		// display(jumpCount);
		return jumpCount[N - 1];
	}
	
	private static int[] getJumpCntArray(int r1, int r2, int N, char[] cells ) {
		int[] jumpCount = new int[N];
		
		for (int i = 0; i < jumpCount.length; i++) {
			jumpCount[i] = N + 1;
		}
		
		if (cells[0] == safe) 
			jumpCount[0] = 0;

		int[] primeCounts = primeCounts(N);

		int i = 0;
		while (i < jumpCount.length) {
			int A = randomJump(i + 1, r1, r2, primeCounts, cells);
			int maxJump = A > 2 ? A : 2;
			for (int j = i + 1; j < N && j <= i + maxJump; j++)
				if (cells[j] == safe && jumpCount[j] > jumpCount[i] + 1)
					jumpCount[j] = jumpCount[i] + 1;
			i++;
		}
		return jumpCount;
	}

	private static int[] getJumpCntArray2(int r1, int r2, int N, char[] cells ) {
		int[] jumpCount = new int[N];
		
		for (int i = 0; i < jumpCount.length; i++) {
			jumpCount[i] = N + 1;
		}
		
		if (cells[0] == safe) 
			jumpCount[0] = 0;

		int[] primeCounts = primeCounts(N);

		int i = 0;
		while (i < jumpCount.length) {
			int A = randomJump(i + 1, r1, r2, primeCounts, cells);
			if (i + 1 < N && cells[i + 1] == safe && jumpCount[i + 1] > jumpCount[i] + 1)
				jumpCount[i + 1] = jumpCount[i] + 1;
			if (i + 2 < N && cells[i + 2] == safe && jumpCount[i + 2] > jumpCount[i] + 1)
				jumpCount[i + 2] = jumpCount[i] + 1;
			if (i + A < N && cells[i + A] == safe && jumpCount[i + A] > jumpCount[i] + 1)
				jumpCount[i + A] = jumpCount[i] + 1;

			i++;
		}
		return jumpCount;
	}

	static int randomJump(int curr, int r1, int r2, int[] primeCounts, char [] S) {
		double c1 = (double)r1/r2;
		int N = primeCounts.length;
		int A = primeCounts[curr];
		double c2 = (double)A/curr;
		if(c2 >= c1 && curr + A < N) {
			return A;
		}
		return 0;
	}
	
	private static int [] primeCounts(int max) {
		boolean [] nonPrimeFlags = generate(max);
		int [] counts = new int [nonPrimeFlags.length];
		int count = 0;
		for (int i = 0; i < nonPrimeFlags.length; i++) {
			if(!nonPrimeFlags[i]) {
				counts[i]=++count;
			}else {
				counts[i]=count;
			}	
		}
		return counts;
	}
	
	private static boolean [] generate(int max) {
		boolean [] nonPrimeFlags = new boolean [max+1];
		nonPrimeFlags[0] = true;
		nonPrimeFlags[1] = true;
		int next = 2;
		while(next <= Math.sqrt(max)) {
			for(int i= next*next; i<=max; i+=next) {
				nonPrimeFlags[i]=true;
			}
			next = getNextPrime(next, nonPrimeFlags);
		}
		return nonPrimeFlags;
	}

	private static int getNextPrime(int next, boolean[] nonPrimeFlags) {
		for (int i = next+1; i < nonPrimeFlags.length; i++) {
			if(!nonPrimeFlags[i])
				return i;
		}
		return -1;
	}
	
	
	//---------------------------------------------------------------------------------
	//---------------------------------------------------------------------------------
	//---------------------------------------------------------------------------------
	
	static void display(int[] arr) {
		for (int i : arr) {
			System.out.println(i);
		}
	}
	
	static void test() {
		boolean[] nonPrimeFlags = generate(50);
		for (int i = 0; i < nonPrimeFlags.length; i++) {
			if (!nonPrimeFlags[i])
				System.out.print(i + ", ");
		}
		System.out.println("\n");
		int[] counts = primeCounts(50);
//		char c = 0x2b9f;
//		for (int i = 0; i < counts.length; i++) {
//			System.out.print(i+"  ");
//			
//		}
//		System.out.print("\n");
//		for (int i = 0; i < counts.length; i++) {
//			System.out.print(c);
//			System.out.print("    ");
//		}
		System.out.print("\n");
		for (int i = 0; i < counts.length; i++) {
			System.out.print(counts[i] + ", ");
		}
		System.out.println("\n");

	}
	
//	static int solve(int r1, int r2, int N, String S){
//		int [] jumpCount = new int [N];
//		char [] cells = S.toCharArray();
//		if(cells[0] == safe) jumpCount[0] = 1; else jumpCount[0]=N+1;
//		if(cells[1] == safe) jumpCount[1] = 1; else jumpCount[1]=N+1;
//		int curPos = 1;
//		for (int i = 2; i < jumpCount.length; i++) {
//			jumpCount[i]=N+1;
//		}
//		
//		for (int i = 2; i < jumpCount.length; i++) {
//			int count = N + 1;
//			if (cells[i] == safe)
//				if (jumpCount[i - 1] < jumpCount[i - 2])
//					count = jumpCount[i - 1] + 1;
//				else
//					count = jumpCount[i - 2] + 1;
//			if(count <= N)
//			  jumpCount[i] = count;
//		}
//		display(jumpCount);
//		return jumpCount[N-1];
//		
//	}
}
