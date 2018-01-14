package test;

public class Test2 extends Test1{
	
	public Test2() {
		System.out.println("hi");
	}
	
	{
		System.out.println("hello");
	}
public static void main(String[] args) {
	Test2 t  = new Test2();
	Test2 t1  = new Test2();
}
}
