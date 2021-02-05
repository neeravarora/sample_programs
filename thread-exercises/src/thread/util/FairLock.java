package thread.util;

import java.util.LinkedList;
import java.util.Queue;

public class FairLock {

	private boolean isLocked = false;
	private Thread lockedBy = null;
	
	private Queue<Lock> queue = new LinkedList<>();
	
	public void lock() throws InterruptedException {
		Lock lock = new Lock();
		synchronized (this) {
			queue.add(lock);
		}
		
		while(isLocked || lock != queue.peek()) {
			synchronized (this) {
				// It will be executed for first thread
				if (!isLocked && lock == queue.peek()) {
					isLocked = true;
					lockedBy = Thread.currentThread();
					queue.remove();
				}
				return;
			}
			
		}
		//
		lock.lock();
		
		
	}
	
	public void unlock() throws InterruptedException {
		if (this.lockedBy != Thread.currentThread()) {
			throw new IllegalMonitorStateException("Calling thread has not locked this lock");
		}
		isLocked      = false;
	    lockedBy = null;
	    if(queue.size() > 0){
	      queue.peek().unlock();
	    }
		
	}
}
