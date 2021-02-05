package thread.util;

public class Lock {

	private Boolean isLocked = false;
	private Thread lockedBy = null;
	
	public void lock() throws InterruptedException {
		synchronized (this) {
			
			while(isLocked) {
				wait();
			}
			lockedBy = Thread.currentThread();
			isLocked = true;
		}
	}
	
	
	public void unlock() throws InterruptedException {
		synchronized (this) {
			if (lockedBy == Thread.currentThread()) {
				isLocked = false;
				lockedBy = null;
				notifyAll();
			}else {
				throw new IllegalMonitorStateException();
			}
		}
	}
}
