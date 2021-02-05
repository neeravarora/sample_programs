import java.util.HashMap;
import java.util.Map;

public class Main {

	static Object o1 = new Object();
	static Object o2 = new Object();
	static Object o3 = new Object();

	static boolean flag = false;

	public static void main(String[] args) throws InterruptedException {
		// Integer [] arr = new Integer [3];
		// System.out.println("hi");
//		 Map<Integer, Integer> map = new HashMap<>();
//		 System.out.println(map.keySet());
//		 map.put(1, null);
//		
//
//		 System.out.println("hi"+map.size());
//		 map.remove(1);
//		 System.out.println("hi"+map.size());
//		 map.put(2, null);

//		notify1();
		t3.start();
		t1.start();
		t2.start();

	}

	static Thread t1 = new Thread() {

		@Override
		public void run() {
			synchronized (o1) {
				try {
					flag = true;
					o1.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				print();
				notify2();
			}
		}
	};

	static Thread t2 = new Thread() {

		@Override
		public void run() {
			synchronized (o2) {
				try {

					o2.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				print2();
				notify3();;
			}

		}
	};

	static Thread t3 = new Thread() {

		@Override
		public void run() {
			//try {
//				while (!flag) {
//
//					//Thread.sleep(4l);
//					System.out.println(flag);
//
//				}
				//Thread.sleep(4000l);
				
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}

			synchronized (o3) {
				try {
					while (!flag) {
						System.out.println(flag);
					}
					notify1();
					o3.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				print3();
			}

		}
	};

	private static void print() {

		for (int i = 0; i < 1000; i++) {
			System.out.println("t1: " + i);
		}

	}

	private static void print2() {
		for (int i = 1000; i < 2000; i++) {
			System.out.println("t2: " + i);
		}
	}

	private static void print3() {
		for (int i = 2000; i < 3000; i++) {
			System.out.println("t3: " + i);
		}
	}

	private static void notify1() {
		synchronized (o1) {
			o1.notify();
		}

	}

	private static void notify2() {
		synchronized (o2) {
			o2.notify();
		}

	}

	private static void notify3() {
		synchronized (o3) {
			o3.notify();
		}

	}
}
