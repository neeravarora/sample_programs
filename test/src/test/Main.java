package test;

public class Main {
	
	interface A {
		void hi();
	}

	static class B implements A {

		@Override
		public void hi() {
			System.out.println("B");

		}
	}

	static class T {
		void test(A a) {
			System.out.println("T");
		}

	}

	static class TD extends T {

		public void test(B b) {
			System.out.println("TD");

		}
	}

	public static void main(String[] args) {
		A a = new B();
		TD t = new TD();
		t.test(a);
	}
}
