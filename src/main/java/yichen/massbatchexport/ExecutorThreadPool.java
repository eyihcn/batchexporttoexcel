package yichen.massbatchexport;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorThreadPool {
		private ExecutorService executor;

		public ExecutorThreadPool(int corePoolSize,int maximumPoolSize) {
			this.executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 30L, TimeUnit.SECONDS,
					new LinkedBlockingQueue<Runnable>());
		}
		
		/**
		 *	如果 任务总数小于corePoolSize，则corePoolSize=任务总数，maximumPoolSize=任务总数
		 * 否则默认 corePoolSize= cpus核数*3，maximumPoolSize=25
		 * @param taskCounts 任务总数 
		 */
		public ExecutorThreadPool(int taskCounts) {
			int corePoolSize = (int)(Runtime.getRuntime().availableProcessors() * 3);
			int maximumPoolSize = 25;
			if (taskCounts<corePoolSize) {
				corePoolSize = taskCounts;
				maximumPoolSize = taskCounts;
			}
			System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ corePoolSize = " +corePoolSize);
			System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ maximumPoolSize = " +maximumPoolSize);
			this.executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 60L, TimeUnit.SECONDS,
					new LinkedBlockingQueue<Runnable>());
		}

		public void execute(Runnable task) {
			executor.execute(task);
		}

		public void execute(Collection<? extends Runnable> tasks) {
			if (null != tasks) {
				for (Runnable task : tasks) {
					execute(task);
				}
			}
		}
		
		public void shutDown() {
			executor.shutdown();
		}
	}