package test;

import java.util.stream.IntStream;

/** 
* 
* Google OnSite Round 2 
* 
* Input is an integer array A 
* Return an array B such that B[i] = product of all elements of A except A[i] 
* 
*/
public class ProductOfAllExceptSelf 
{
	public double[] getProducts(int[] input) throws Exception
	{
		if(input == null || input.length < 2)
			throw new Exception("Invalid array - at least 2 elements needed");
		else if(input.length == 2)
		{
			return new double[]{input[1], input[0]};
		}
		else
		{
			// check if zero exists
			int N = input.length;
			double[] result = new double[N];
			int i, zeroCount = 0, zeroIndex = -1;
			for(i = 0; i < N; i++)
			{
				if(input[i] == 0)
				{
					++zeroCount;
					zeroIndex = i;
				}
			}
			
			switch(zeroCount)
			{
			case 0:
				// no zeros
				double[] CP = new double[N];
				CP[0] = input[0];
				
				for(i = 1; i < N; i++)
				{
					CP[i] = CP[i - 1] * input[i];
				}
				
				for(i = 0; i < N; i++)
				{
					if(i == 0)
						result[i] = CP[N - 1] / CP[0];
					else if(i == N - 1)
						result[i] = CP[N - 2];
					else
						result[i] = (CP[N - 1]*CP[i - 1])/CP[i];
				}
				
				break;
				
			case 1:
				// exactly one element is zero
				int product = 1;
				for(i = 0; i < N; i++)
				{
					if(i != zeroIndex)
					{
						product *= input[i];
					}
					else
					{
						result[i] = 0;
					}
				}
				
				result[zeroIndex] = product;
				break;

			default:
				// 2 or more zeros
				for(i = 0; i < N; i++)
				{
					result[i] = 0;
				}
				
				break;
			}
			
			return result;
		}
	}
	
	
	public static int[] productOfAllElementsExceptNth(int[] a) {
        return IntStream.range(0, a.length)
                .map(i -> IntStream.range(0, a.length)
                        .filter(j -> j!=i)
                        .map(j -> a[j])
                        .reduce(1, (x, y) -> x * y))
                .toArray();
    }
	
	
}


