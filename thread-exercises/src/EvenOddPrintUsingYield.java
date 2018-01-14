
public class EvenOddPrintUsingYield {

	static volatile boolean isOddPrinted = false;

	public static void main(String[] args) {
		t1.start();
		t2.start();
	}

	static Thread t1 = new Thread() {

		@Override
		public void run() {
			try {
				oddPrint();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	};

	static Thread t2 = new Thread() {

		@Override
		public void run() {
			try {
				evenPrint();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	};

	private static  void evenPrint() throws InterruptedException {

		for (int i = 2; i <= 100; i = i + 2) {
			while (!isOddPrinted)
				Thread.yield();
			System.out.println("t2: " + i);
			isOddPrinted = false;
		}

	}

	private static  void oddPrint() throws InterruptedException {
		for (int i = 1; i < 100; i = i + 2) {
			while (isOddPrinted) {
				Thread.yield();
			}

			System.out.println("t1: " + i);
			isOddPrinted = true;
		}
	}

}
