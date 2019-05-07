package gate.controllers;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class ExecutorQueue {

	private ExecutorService executor;
	private int maxParallelTasks;

	private int submittedTasks = 0;
	private Collection<Iterator<Runnable>> tasksQueue = new LinkedHashSet<>();
	private Map<RunnableTask, Future<?>> futures = new LinkedHashMap<>();

	public ExecutorQueue(ExecutorService executor, int maxSubmittedTasks) {
		this.executor = executor;
		this.maxParallelTasks = maxSubmittedTasks;
	}

	public synchronized void submit(Iterator<Runnable> tasks) {
		tasksQueue.add(tasks);

		executeNext();
	}

	private synchronized void executeNext() {
		while (submittedTasks < maxParallelTasks) {
			Iterator<Iterator<Runnable>> taskQueueIterator = tasksQueue.iterator();
			if (taskQueueIterator.hasNext()) {
				Iterator<Runnable> tasksIterator = taskQueueIterator.next();
				if (tasksIterator.hasNext()) {
					submittedTasks++;
					RunnableTask runnableTask = new RunnableTask(this, tasksIterator.next());
					Future<?> submit = executor.submit(runnableTask);
					futures.put(runnableTask, submit);
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
		while (!futures.isEmpty()) {
			Future<?> future = null;
			synchronized (this) {
				if (!futures.isEmpty()) {
					Iterator<Entry<RunnableTask, Future<?>>> iterator = futures.entrySet().iterator();
					future = iterator.next().getValue();
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
