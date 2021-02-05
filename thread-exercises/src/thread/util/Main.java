package thread.util;

public class Main {
	
	private static final Lock lock = new Lock();
	
	
	public static void main(String[] args) {
		
		Runnable task = new Runnable() {

			@Override
			public void run() {
				try {
					lock.lock();
					// Critical section
					for (int i = 0; i < 10; i++) {
						System.out.println(Thread.currentThread().getName() + " ::: "+i);
					}
					//
					lock.unlock();
				} catch (InterruptedException | 
						RuntimeException  e) {
					e.printStackTrace();
				}
			}
		};
		
		Thread t = new Thread(task);
		Thread t2 = new Thread(task);
		
		t.start();
		t2.start();
	}

}
