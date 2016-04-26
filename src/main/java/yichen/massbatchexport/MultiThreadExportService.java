package yichen.massbatchexport;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

/**
 * 批量导入execel时，分页处理，使用多线程 eg :
 */
public class MultiThreadExportService<T, D> {

	public static int DEFAULT_PAGE_SIZE = 5000;
	public static int DEFAULT_ROW_SIZE_OF_PERSHEET = 10000;

	private int totalCounts; // 总记录数
	private int pageSize;// 一页查询的数据量
	private int rowSizeOfPersheet;// 一个sheet多少行
	private int sheetCounts; // 多少页sheet
	private int totalTaskCounts;// 任务总数
	private HSSFWorkbook workbook;
	private HSSFSheet[] sheets;

	private String[] headerRowData; // 标题集合
	String queryMethodName; // dao的查询方法
	Map<String, Object> queryParam;
	Map<String, Object> sort;
	DataProvider<T, D> dataProvider;
	RowDataHandler<T> rowDataHandler;
	Class<D> daoClass;
	AtomicInteger leftTaskCounts;
	ConcurrentHashMap<Integer, List<T>> cacheEntities = new ConcurrentHashMap<Integer, List<T>>();
	AtomicReferenceArray<Lock> sheetLocks; // 细化锁的粒度到每一个sheet？？
	Lock workbookLock = new ReentrantLock();
	boolean useReflect = true;
	private List<ExportTask<T, D>> tasks = new ArrayList<ExportTask<T, D>>();

	AtomicInteger failCounts = new AtomicInteger(0); // 处理时异常，创建行失败
	AtomicInteger blankRowCounts = new AtomicInteger(0); // 业务过滤掉的行

	/**
	 * 指定使用反射查询数据 默认的pageSize为5000，一个sheet10000条记录, 传入的rowDataHandler
	 * 将会被多线程调用，注意线程安全的问题
	 */
	public MultiThreadExportService(int totalCounts, String[] headerRowData, String queryMethodName, Map<String, Object> queryParam, Map<String, Object> sort, Class<D> daoClass,
			RowDataHandler<T> rowDataHandler) {
		this(totalCounts, DEFAULT_PAGE_SIZE, DEFAULT_ROW_SIZE_OF_PERSHEET, true, headerRowData, daoClass, queryMethodName, queryParam, sort, null, rowDataHandler);
	}

	/**
	 * 不使用反射，自己实现DateProvider接口，一页查询的数据量和每个sheet的容量采用默认值 ,
	 * 传入的rowDataHandler 和  dataProvider 将会被多线程调用，注意线程安全的问题
	 */
	public MultiThreadExportService(int totalPageSize, String[] headerRowData, Class<D> daoClass, DataProvider<T, D> dataProvider, RowDataHandler<T> rowDataHandler) {
		this(totalPageSize, DEFAULT_PAGE_SIZE, DEFAULT_ROW_SIZE_OF_PERSHEET, false, headerRowData, daoClass, null, null, null, dataProvider, rowDataHandler);
	}

	/**
	 * @param totalCounts
	 *            总记录数
	 * @param pageSize
	 *            一页查询数
	 * @param rowSizeOfPersheet
	 *            一个sheet有多少行记录
	 * @param useReflectToQuery
	 *            是否使用反射查询数据
	 * @param headerRowData
	 *            标题
	 * @param daoClass
	 *            查询Dao的运行时类
	 * @param queryMethodName
	 *            查询的方法名称
	 * @param queryParam
	 *            查询参数
	 * @param sort
	 *            排序参数
	 * @param dataProvider
	 * @param rowDataHandler
	 *            业务处理的实现，返回一行数据
	 */
	private MultiThreadExportService(int totalCounts, int pageSize, int rowSizeOfPersheet, boolean useReflectToQuery, String[] headerRowData, Class<D> daoClass, String queryMethodName,
			Map<String, Object> queryParam, Map<String, Object> sort, DataProvider<T, D> dataProvider, RowDataHandler<T> rowDataHandler) {
		super();
		if (totalCounts < 1 || pageSize < 0 || rowSizeOfPersheet < 0 || "".equals(queryMethodName)) {
			throw new IllegalArgumentException();
		}
		this.pageSize = pageSize;
		this.totalCounts = totalCounts;
		this.rowSizeOfPersheet = rowSizeOfPersheet;
		this.headerRowData = headerRowData;
		this.useReflect = useReflectToQuery;
		this.rowDataHandler = rowDataHandler;
		sheetCounts = caculateSheetCounts(totalCounts, rowSizeOfPersheet);
		// 每一个sheet分配一把锁
		// this.sheetLocks = initSheetLocks(sheetCounts);

		this.daoClass = daoClass;
		if (useReflectToQuery == true) {
			this.queryMethodName = queryMethodName;
			this.queryParam = queryParam;
			this.sort = sort;
		} else {
			this.dataProvider = dataProvider;
		}
	}

	private int caculateSheetCounts(int totalCounts, int rowSizeOfPersheet) {
		return (int) Math.ceil(((double) totalCounts) / rowSizeOfPersheet);
	}

	private AtomicReferenceArray<Lock> initSheetLocks(int sheetCounts) {
		AtomicReferenceArray<Lock> sheetLocks = new AtomicReferenceArray<Lock>(sheetCounts);
		for (int index = 0; index < sheetCounts; index++) {
			sheetLocks.set(index, new ReentrantLock());
		}
		return sheetLocks;
	}

	/**
	 * 从数据库中导出到目标execel文件，返回目标文件的输出流
	 * 
	 * @param exportFileName
	 *            导出文件名称
	 * @return
	 */
	public FileInputStream export(String exportFileName) {

		workbook = new HSSFWorkbook();
		// 根据总的记录数量 ,计算所需的sheet
		createSheets();
		// 插入标题
		buildTitles();

		FileInputStream fin = null;
		FileOutputStream fout = null;
		ExecutorThreadPool executorThreadPool = null;

		try {
			divideSheetsToTask();
			if (totalTaskCounts <= 0) {
				return null;
			}
			leftTaskCounts = new AtomicInteger(totalTaskCounts);
			if (totalTaskCounts > 1) {
				executorThreadPool = new ExecutorThreadPool(totalTaskCounts);
				executorThreadPool.execute(tasks);
			} else {
				new Thread(tasks.get(0)).start();
			}
			int left = leftTaskCounts.get();
			while (left > 0) {
				System.out.println("left task counts ================ > " + left);
				Thread.yield();
				Thread.sleep(500); // 调用线程休眠0.5s,等待工作线程执行完成
				left = leftTaskCounts.get();
			}
			fout = new FileOutputStream(exportFileName);
			workbook.write(fout);
			fin = new FileInputStream(new File(exportFileName));
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			if (null != executorThreadPool) {
				executorThreadPool.shutDown();
				executorThreadPool = null;
			}
			if (null != fout) {
				try {
					fout.close();
				} catch (IOException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
				fout = null;
			}
		}
		return fin;
	}

	/*
	 * 将 数据 和 sheet的行 分给task (每个task只关联一个sheet) 分两种情况： 1. rowSizeOfPersheet >=
	 * pageSize (一页的数据大于等每个sheet的行数) 2. rowSizeOfPersheet < pageSize
	 */
	private void divideSheetsToTask() {

		if (rowSizeOfPersheet >= pageSize) {
			int prePageLeft = 0; // 上一页数据的剩余
			int pageNumber = 0;
			int sheetIndex = -1;
			// 注意 循环的关注点在sheetIndex 和 pageNumber
			while (true) {
				int tempTotal = pageNumber * pageSize;
				if (tempTotal >= totalCounts) {
					// 若上一页的数据有剩余，则sheetIndex++,则可以使用当前页数据的缓存，新建一个task
					if (prePageLeft > 0) {
						int more = tempTotal - totalCounts;
						// 上一页的剩余数据比more大，则还需要新建sheet
						if (prePageLeft > more) {
							sheetIndex++;
							tasks.add(new ExportTask<T, D>(sheetIndex, 1, prePageLeft - more, pageSize - prePageLeft + 1, pageNumber, pageSize, false, true, sheets[sheetIndex], this));
						}
					}
					// 若prePageLeft == 0，不需要新建sheet了，之前的sheets已经够导出数据了
					break;
				} else { // pageNumber*pageSize < totalPageSize
					if (prePageLeft > 0) {
						// 上一页的数据有剩余，新建sheet
						sheetIndex++;
						tasks.add(new ExportTask<T, D>(sheetIndex, 1, prePageLeft, pageSize - prePageLeft + 1, pageNumber, pageSize, false, true, sheets[sheetIndex], this));
						// 这时需要查询下一页的数据
						pageNumber++;
						// 当前sheet的剩余rows
						int newSheetLeftRows = rowSizeOfPersheet - prePageLeft;
						// 剩余的总数据量
						int totalPageSizeLeft = totalCounts - tempTotal;
						// 比较剩余rows 和 一页数据pageSize的大小
						// 1. 比较剩余rows 大于等于 一页的数据
						if (newSheetLeftRows >= pageSize) {
							int times = newSheetLeftRows / pageSize;
							int taskNum = 0;
							int fromRowIndex = 0;
							int endRowIndex = 0;
							for (; taskNum < times; taskNum++) {
								if (taskNum > 0) {
									pageNumber++;
								}
								fromRowIndex = taskNum * pageSize + 1 + prePageLeft;
								endRowIndex = fromRowIndex - 1 + pageSize;
								// 剩余数据总量少于一页，查询当页，跳出
								if (totalPageSizeLeft < pageSize) {
									break; // for
								}
								tasks.add(new ExportTask<T, D>(sheetIndex, fromRowIndex, endRowIndex, 0, pageNumber, pageSize, true, false, sheets[sheetIndex], this));

								// 剩余数据总量减少一页，pageNumber++
								totalPageSizeLeft -= pageSize;
							}
							// 判断跳出for的条件， 剩余数据总量少于一页，查询当页，跳出
							if (taskNum < times) {
								tasks.add(new ExportTask<T, D>(sheetIndex, fromRowIndex, fromRowIndex - 1 + totalPageSizeLeft, 0, pageNumber, pageSize, false, false, sheets[sheetIndex], this));
								break;
							}
							// 若上面的for正常结束，则当前sheet的剩余rows小于pageSize
							newSheetLeftRows = newSheetLeftRows % pageSize;
							if (newSheetLeftRows == 0) { // 刚好整除
								if (totalPageSizeLeft == 0) {// 刚好剩余总数为0
									break;
								}
								// totalPageSizeLeft 大于0， 需要进入下一个sheet
								prePageLeft = 0;
								continue;
							}
							// 若newSheetLeftRows>0, 一定小于 pageSize 需要查询下一页
							pageNumber++;
							// 此时，剩余总量还是未知
							// 若，总剩余 大于等于一页,newSheetLeftRows 小于一页
							if (totalPageSizeLeft >= pageSize) {
								tasks.add(new ExportTask<T, D>(sheetIndex, endRowIndex + 1, rowSizeOfPersheet, 0, pageNumber, pageSize, true, false, sheets[sheetIndex], this));
								prePageLeft = pageSize - newSheetLeftRows;
								continue;// 当前sheet结束 ，需要进入下一个sheet
							}
							// totalPageSizeLeft < pageSize， newSheetLeftRows <
							// pageSize
							// 当前sheet剩余的rows ，足够容纳剩余总量
							if (newSheetLeftRows >= totalPageSizeLeft) {
								tasks.add(new ExportTask<T, D>(sheetIndex, endRowIndex + 1, endRowIndex + totalPageSizeLeft, 0, pageNumber, pageSize, false, false, sheets[sheetIndex], this));
								break; // 结束
							}
							// newSheetLeftRows < totalPageSizeLeft < pageSize
							// 当前sheet的剩余rows，无法容纳 剩余总量
							tasks.add(new ExportTask<T, D>(sheetIndex, endRowIndex + 1, rowSizeOfPersheet, 0, pageNumber, pageSize, true, false, sheets[sheetIndex], this));
							prePageLeft = pageSize - newSheetLeftRows;
							continue; // 当前sheet结束 ，需要进入下一个sheet
						} // end for newSheetLeftRows >= pageSize
							// newSheetLeftRows < pageSize
						if (totalPageSizeLeft < pageSize) {
							if (totalPageSizeLeft >= newSheetLeftRows) {
								tasks.add(new ExportTask<T, D>(sheetIndex, prePageLeft + 1, rowSizeOfPersheet, 0, pageNumber, pageSize, true, false, sheets[sheetIndex], this));
								prePageLeft = pageSize - newSheetLeftRows;
								continue; // 当前sheet结束
							}
							// totalPageSizeLeft < newSheetLeftRows
							tasks.add(new ExportTask<T, D>(sheetIndex, prePageLeft + 1, prePageLeft + totalPageSizeLeft, 0, pageNumber, pageSize, false, false, sheets[sheetIndex], this));
							break;
						}
						// totalPageSizeLeft >= pageSize > newSheetLeftRows
						// 当前sheet不足容纳剩余总量
						tasks.add(new ExportTask<T, D>(sheetIndex, prePageLeft + 1, rowSizeOfPersheet, 0, pageNumber, pageSize, true, false, sheets[sheetIndex], this));
						prePageLeft = pageSize - newSheetLeftRows;
						continue;
					}
					// prePaegLeft == 0
					// pageNumber*pageSize < totalPageSize
					sheetIndex++;
					pageNumber++;
					// 剩余的总数据量
					int totalPageSizeLeft = totalCounts - tempTotal;
					// 剩余总量 大于 sheet的rows > pageSize
					if (totalPageSizeLeft >= rowSizeOfPersheet) {
						int fromRowIndex = 0;
						int endRowIndex = 0;
						// rowSizeOfPersheet 一定大于等于 pageSize
						int times = rowSizeOfPersheet / pageSize;
						for (int taskNum = 0; taskNum < times; taskNum++) {
							if (taskNum > 0) {
								pageNumber++;
							}
							fromRowIndex = 1 + taskNum * pageSize;
							endRowIndex = fromRowIndex + pageSize - 1;
							tasks.add(new ExportTask<T, D>(sheetIndex, fromRowIndex, endRowIndex, 0, pageNumber, pageSize, true, false, sheets[sheetIndex], this));
						}
						// 计算余数
						int mod = rowSizeOfPersheet % pageSize;
						if (mod == 0) { // 一个sheet的rows刚好被几页数据平分
							prePageLeft = 0;
							continue;
						}
						// 有余数，需要查询下一页数据
						pageNumber++;
						tasks.add(new ExportTask<T, D>(sheetIndex, endRowIndex + 1, rowSizeOfPersheet, 0, pageNumber, pageSize, true, false, sheets[sheetIndex], this));
						prePageLeft = pageSize - mod;
						continue;
					} // end for totalPageSizeLeft >= rowSizeOfPersheet
						// pageNumber*pageSize < totalPageSize
						// totalPageSizeLeft < rowSizeOfPersheet
						// rowSizeOfPersheet > pageSize
						// prePaegLeft == 0
					int times = totalPageSizeLeft / pageSize;
					if (times == 0) { // totalPageSizeLeft < pageSize
						tasks.add(new ExportTask<T, D>(sheetIndex, 1, totalPageSizeLeft, 0, pageNumber, pageSize, false, false, sheets[sheetIndex], this));
						break;
					}
					// totalPageSizeLeft < rowSizeOfPersheet
					// totalPageSizeLeft >= pageSize
					int fromRowIndex = 0;
					int endRowIndex = 0;
					for (int taskNum = 0; taskNum < times; taskNum++) {
						if (taskNum > 0) {
							pageNumber++;
						}
						fromRowIndex = 1 + taskNum * pageSize;
						endRowIndex = fromRowIndex + pageSize - 1;
						tasks.add(new ExportTask<T, D>(sheetIndex, fromRowIndex, endRowIndex, 0, pageNumber, pageSize, false, false, sheets[sheetIndex], this));
					}
					// 计算余数
					int mod = totalPageSizeLeft % pageSize;
					// (mod == 0) totalPageSizeLeft刚好被几页数据平分
					// 有余数，需要查询下一页数据
					if (mod != 0) {
						pageNumber++;
						tasks.add(new ExportTask<T, D>(sheetIndex, endRowIndex + 1, endRowIndex + mod, 0, pageNumber, pageSize, false, false, sheets[sheetIndex], this));
					}
					break;
				}
			}
			totalTaskCounts = tasks.size();
			return;
		}
		// rowSizeOfPersheet < pageSize
		int pageNumber = 0;
		int sheetIndex = -1;
		int prePageLeft = 0; // 上一页查询数据的剩余量
		int totalPageLeft = totalCounts;
		while (true) {
			// 剩余总量已经为0
			if (totalPageLeft == 0) {
				break;
			}
			// 剩余总量不为0， 上一页数据有剩余，建立新的sheet
			if (prePageLeft > 0) {
				// 新建sheet
				sheetIndex++;
				if (totalPageLeft <= prePageLeft) {
					if (totalPageLeft >= rowSizeOfPersheet) {

						tasks.add(new ExportTask<T, D>(sheetIndex, 1, rowSizeOfPersheet, prePageLeft + 1, pageNumber, pageSize, false, true, sheets[sheetIndex], this));
						prePageLeft -= rowSizeOfPersheet;
						totalPageLeft -= rowSizeOfPersheet;
						continue;
					}
					// totalPageLeft < rowSizeOfPersheet
					tasks.add(new ExportTask<T, D>(sheetIndex, 1, totalPageLeft, prePageLeft + 1, pageNumber, pageSize, false, true, sheets[sheetIndex], this));
					totalPageLeft = 0;
					break;
				}
				// totalPageLeft > prePageLeft
				// prePageLeft < pageSize
				// rowSizeOfPersheet < pageSize
				// 上一页剩余的数据 小于 当前sheet的行数
				if (prePageLeft < rowSizeOfPersheet) {
					tasks.add(new ExportTask<T, D>(sheetIndex, 1, prePageLeft, prePageLeft + 1, pageNumber, pageSize, false, true, sheets[sheetIndex], this));
					totalPageLeft -= prePageLeft;
					// 此时 totalPageLeft 一定大于 0，所以需要下一页的数据查询
					pageNumber++;// prePageLeft < rowSizeOfPersheet
					int currSheetLeft = rowSizeOfPersheet - prePageLeft;
					// 剩余数据总量 >= 当前sheet的剩余量,需要查询下一页数据
					if (totalPageLeft >= currSheetLeft) {
						// pageSize > rowSizeOfPersheet > currSheetLeft
						tasks.add(new ExportTask<T, D>(sheetIndex, 1 + prePageLeft, rowSizeOfPersheet, 0, pageNumber, pageSize, true, false, sheets[sheetIndex], this));
						prePageLeft = pageSize - currSheetLeft;
						totalPageLeft -= currSheetLeft;
						continue; // 当前sheet已填充完成，转入下一个sheet
					}
					// 0<>totalPageLeft < currSheetLeft
					tasks.add(new ExportTask<T, D>(sheetIndex, 1 + prePageLeft, prePageLeft + totalPageLeft, 0, pageNumber, pageSize, true, false, sheets[sheetIndex], this));
					totalPageLeft = 0;
					break;
				} else {
					// prePageLeft >= rowSizeOfPersheet
					tasks.add(new ExportTask<T, D>(sheetIndex, 1, rowSizeOfPersheet, prePageLeft + 1, pageNumber, pageSize, false, true, sheets[sheetIndex], this));
					prePageLeft -= rowSizeOfPersheet; // 上一页数据剩余
					totalPageLeft -= rowSizeOfPersheet; // 总数据量剩余
					continue; // 需要进入下一个sheet
				}
			} // end for prePageLeft > 0

			// rowSizeOfPersheet < pageSize
			// prePageLeft == 0,totalPageLeft > 0
			// 这时需要新建sheet,查询下一页数据
			sheetIndex++;
			pageNumber++;
			// 剩余数据总量 >= 一页sheet的行数
			if (totalPageLeft >= rowSizeOfPersheet) {
				tasks.add(new ExportTask<T, D>(sheetIndex, 1, rowSizeOfPersheet, 0, pageNumber, pageSize, true, false, sheets[sheetIndex], this));
				prePageLeft = pageSize - rowSizeOfPersheet;
				totalPageLeft -= rowSizeOfPersheet;
				continue;// 需要进入下一个sheet
			}
			// totalPageLeft < rowSizeOfPersheet
			// 一页就可以查询出所有的剩余数据
			tasks.add(new ExportTask<T, D>(sheetIndex, 1, totalPageLeft, 0, pageNumber, pageSize, false, false, sheets[sheetIndex], this));
			totalPageLeft = 0;
			break;
		}
		totalTaskCounts = tasks.size();
	}

	/**
	 * 创建所需的sheet
	 */
	private void createSheets() {
		sheets = new HSSFSheet[sheetCounts];
		for (int counts = 0; counts < sheetCounts; counts++) {
			sheets[counts] = workbook.createSheet();
		}
	}

	/**
	 * 为sheet插入标题行
	 */
	private void buildTitles() {
		HSSFRow row = null;
		HSSFCell cell = null;
		String title = null;
		for (int index = 0, len = sheets.length; index < len; index++) {
			row = sheets[index].createRow(0);
			for (int columnIndex = 0, columnLen = headerRowData.length; columnIndex < columnLen; columnIndex++) {
				cell = row.createCell(columnIndex);
				title = headerRowData[columnIndex];
				if (null == title || title.length() == 0) {
					title = " ";
				}
				cell.setCellValue(title);
			}
		}
	}

	/**
	 * 根据任务总数 和 每个线程处理任务的数量 计算 所需线程的数量 例如： 任务总数为20 ，每个线程处理任务6 ，所需线程数为 4
	 * 
	 * @param totalJobSize
	 *            任务的总数量
	 * @param jobCountsByPerThread
	 *            每个线程处理任务的数量
	 * @return 线程数
	 */
	public int caculateThreadSize(int totalJobSize, int jobCountsByPerThread) {
		if (totalJobSize < 0 || jobCountsByPerThread < 0) {
			throw new IllegalArgumentException();
		}
		return caculateSheetCounts(totalJobSize, jobCountsByPerThread);
	}

	public RowDataHandler<T> getRowDataHandler() {
		return rowDataHandler;
	}

	public MultiThreadExportService<T, D> setPageSize(int pageSize) {
		this.pageSize = pageSize;
		return this;
	}

	public MultiThreadExportService<T, D> setRowSizeOfPersheet(int rowSizeOfPersheet) {
		this.rowSizeOfPersheet = rowSizeOfPersheet;
		this.sheetCounts = caculateSheetCounts(totalCounts, rowSizeOfPersheet);
		return this;
	}

	/**
	 * 若dao的查询是非线程安全，则每个线程需要单独的dao实例完成查询，保证线程安全 return a new Dao instance
	 * 
	 * @return
	 */
	public D getDaoEntity() {
		try {
			return this.daoClass.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

}
