package thread.util;

import java.util.LinkedList;
import java.util.Queue;

public class ProducerConsumer<T> {

	private Queue<T> queue ;
	private int size = 10;
	
	public ProducerConsumer(int size) {
		this.queue = new LinkedList<T>();
		this.size = size;
	}
	
	Object producerLock = new Object();
	Object consumerLock = new Object();
	
	
	public void produce(T t) throws InterruptedException {
		synchronized (producerLock) {
			while(queue.size() >= size)
				wait();
			queue.add(t);
			//If conditional either queue is half or complete full
			consumerLock.notifyAll();
		}
	}
	
	public T consume() throws InterruptedException {
		synchronized (consumerLock) {
			while(queue.isEmpty())
				wait();
			T t = queue.remove();
			//If conditional either queue is half or empty
			producerLock.notifyAll();
			return t;
		}
	}
}
