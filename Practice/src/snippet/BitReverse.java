package snippet;

public class BitReverse {
	public static byte reverse(byte x) {
	    byte b = 0;
	    for (int i = 0; i < 8; ++i) {
	        b<<=1;
	        b|=( x &1);
	        x>>=1;
	      }
	    return b;
	}
	
	static int reverseBits(int x)
    {
        int b = 0;
        while (x != 0)
        {
            b <<= 1;
            b |= ( x & 1);
            x >>= 1;
        }
        return b;
    }
	public static void main(String args[]) {
	   // byteReverseTest();
		//System.out.println(reverse(new Byte("127")));
		printBin(5);
	}

	 static void byteReverseTest() {
		byte[] nums = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 
	            (byte) 0xAA, (byte) 0xFE, (byte) 0xFF };
	    for (byte b : nums) {
	        System.out.printf("%02X=%02X ", b, reverse(b));
	    }
	    System.out.println();
	}
	 
	 static void printBin(int x)
	    {
		 System.out.print("Binary: ");
		  if(x >= 0)
	        while (x > 0)
	        {
	            System.out.print(x & 1);
	            x >>>= 1;
	        }
		  else {
			  
		  }
	    }
}

