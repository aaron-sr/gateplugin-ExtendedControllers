package gate.controllers;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.http.concurrent.BasicFuture;

public class ExecutorQueue {
	private ExecutorService executor;
	private int maxParallelTasks;

	private boolean interrupted = false;
	private int submittedTasks = 0;
	private Collection<Iterator<Runnable>> tasksQueue = new LinkedHashSet<>();
	private Collection<Future<?>> futures = new LinkedHashSet<>();

	public ExecutorQueue(ExecutorService executor, int maxSubmittedTasks) {
		this.executor = executor;
		this.maxParallelTasks = maxSubmittedTasks;
	}

	public synchronized void submit(Iterator<Runnable> tasks) {
		tasksQueue.add(tasks);

		executeNext();
	}

	public synchronized void interrupt() {
		interrupted = true;
	}

	private synchronized void executeNext() {
		while (!interrupted && submittedTasks < maxParallelTasks) {
			Iterator<Iterator<Runnable>> taskQueueIterator = tasksQueue.iterator();
			if (taskQueueIterator.hasNext()) {
				Iterator<Runnable> tasksIterator = taskQueueIterator.next();
				if (tasksIterator.hasNext()) {
					Runnable next = null;
					Exception exception = null;
					try {
						next = tasksIterator.next();
					} catch (Exception e) {
						exception = e;
					}
					if (exception != null) {
						BasicFuture<?> fail = new BasicFuture<Object>(null);
						fail.failed(exception);
						futures.add(fail);
					} else if (next != null) {
						RunnableTask runnableTask = new RunnableTask(this, next);
						Future<?> submit = executor.submit(runnableTask);
						futures.add(submit);
						submittedTasks++;
					}
				} else {
					taskQueueIterator.remove();
					continue;
				}
			} else {
				break;
			}
		}
	}

	private synchronized void finishedTask(RunnableTask runnableTask) {
		submittedTasks--;
		executeNext();
	}

	public void awaitTasksComplete() throws InterruptedException, ExecutionException {
		while ((!interrupted && !tasksQueue.isEmpty()) || (interrupted && !futures.isEmpty())) {
			Future<?> future = null;
			synchronized (this) {
				if (!futures.isEmpty()) {
					Iterator<Future<?>> iterator = futures.iterator();
					future = iterator.next();
					iterator.remove();
				}
			}
			if (future != null) {
				future.get();
			}
		}
	}

	private static class RunnableTask implements Runnable {

		private ExecutorQueue queue;
		private Runnable runnable;

		public RunnableTask(ExecutorQueue queue, Runnable runnable) {
			this.queue = queue;
			this.runnable = runnable;
		}

		@Override
		public void run() {
			runnable.run();
			queue.finishedTask(this);
		}

	}

}
