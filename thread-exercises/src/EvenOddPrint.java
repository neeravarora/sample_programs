
public class EvenOddPrint {

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

	private static synchronized void evenPrint() throws InterruptedException {

		for (int i = 2; i <= 100; i = i + 2) {
			if (!isOddPrinted)
				EvenOddPrint.class.wait();
			System.out.println("t2: " + i);
			isOddPrinted = false;
			EvenOddPrint.class.notify();
		}

	}

	private static synchronized void oddPrint() throws InterruptedException {
		for (int i = 1; i < 100; i = i + 2) {
			if (isOddPrinted) {
				EvenOddPrint.class.wait();
			}

			System.out.println("t1: " + i);
			isOddPrinted = true;
			EvenOddPrint.class.notify();
		}
	}

}
