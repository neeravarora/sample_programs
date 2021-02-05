package thread.util;

import java.util.LinkedList;
import java.util.Queue;

public class BlockingQueue<T> {

	private Queue<T> queue ;
	private int size = 10;
	
	public BlockingQueue(int size) {
		this.queue = new LinkedList<T>();
		this.size = size;
	}
	
	public void enque(T t) throws InterruptedException {
		synchronized (this) {
			while(queue.size() >= size)
				wait();
			queue.add(t);
			notifyAll();
		}
	}
	
	public T deque() throws InterruptedException {
		synchronized (this) {
			while(queue.isEmpty())
				wait();
			T t = queue.remove();
			notifyAll();
			return t;
		}
	}
}
