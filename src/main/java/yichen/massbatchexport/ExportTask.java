package yichen.massbatchexport;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;

@SuppressWarnings("unchecked")
public class ExportTask<T, D> implements Runnable {

	private int sheetIndex; // 标识实例task的所属的sheet, 可以细化锁的力度，每个sheet一把锁
	private int fromRowIndex; // task的开始行
	private int endRowIndex; // task的结束行
	private int dataFromIndex; // task所需数据的起开始下标
	private int pageNumber; // 一页的行数
	private int pageSize;
	private boolean needCache; // 是否为一整页的task
	private boolean canUseCache; // 是否可以使用缓存数据
	private HSSFSheet hssfSheet; // 实例task所属sheet
	private List<T> onePageData;
	private MultiThreadExportService<T, D> multiThreadExportService;

	public ExportTask(int sheetIndex, int fromRowIndex, int endRowIndex, int dataFromIndex, int pageNumber, int pageSize, boolean needCache, boolean canUseCache, HSSFSheet hssfSheet,
			MultiThreadExportService<T, D> multiThreadExportService) {
		super();
		this.sheetIndex = sheetIndex;
		this.fromRowIndex = fromRowIndex;
		this.endRowIndex = endRowIndex;
		this.dataFromIndex = dataFromIndex;
		this.pageNumber = pageNumber;
		this.pageSize = pageSize;
		this.needCache = needCache;
		this.canUseCache = canUseCache;
		this.hssfSheet = hssfSheet;
		this.multiThreadExportService = multiThreadExportService;
	}

	public void run() {
		getOnePageData();
		if (null != onePageData) {
			int end = fromRowIndex + onePageData.size() - 1; // 防止数组下标越界
			if (endRowIndex > end) {
				endRowIndex = end;
			}
			List<String> oneRow = null;
			int listIndex = dataFromIndex;
			for (int rowIndex = fromRowIndex; rowIndex <= end; rowIndex++) {
				// 业务处理完成返回一行的数据
				try {
					oneRow = multiThreadExportService.rowDataHandler.handler(onePageData.get(listIndex));
				} catch (Exception e) {
					System.out.println(new StringBuilder(Thread.currentThread().getName()).append("RowDataHandler业务在处理 第").append(pageNumber).append("页数据的第 ").append(listIndex).append("条数据时异常 ！！！"));
					listIndex++;
					multiThreadExportService.failCounts.incrementAndGet();
					e.printStackTrace();
					continue;
				}
				listIndex++;
				if (null == oneRow || oneRow.size() == 0) {
					multiThreadExportService.blankRowCounts.incrementAndGet();
					continue; // 业务处理返回空行
				}
				// 锁的粒度细化到每一个sheet还存在问题，暂时锁住整个workbook
				multiThreadExportService.workbookLock.lock();
				try {
					buildOneRow(oneRow, rowIndex);
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println(new StringBuilder("error :构建row失败： sheetIndex =").append(sheetIndex).append(" rowIndex =").append(rowIndex));
					multiThreadExportService.failCounts.incrementAndGet();
				}
				multiThreadExportService.workbookLock.unlock();
			}
		}
		int count = multiThreadExportService.leftTaskCounts.decrementAndGet();
		System.out.println(Thread.currentThread().getName() + " 完成一页 ====================》 还剩 " + count + "页");
	}

	private void getOnePageData() {
		try {
			if (canUseCache) {
				System.out.println("============================= useCache pageNumber=  " + pageNumber);
				onePageData = multiThreadExportService.cacheEntities.get(pageNumber);
			} else {
				if (multiThreadExportService.useReflect) {
					Method declaredMethod = multiThreadExportService.daoClass.getDeclaredMethod(multiThreadExportService.queryMethodName, Map.class, Map.class, Map.class);
					onePageData = (List<T>) declaredMethod.invoke(multiThreadExportService.getDaoEntity(), multiThreadExportService.queryParam, multiThreadExportService.sort,
							buildPagenation(pageSize, pageNumber));
				} else {
					onePageData = multiThreadExportService.dataProvider.providerOnePageDage(multiThreadExportService.getDaoEntity(), pageSize, pageNumber);
				}
				if (needCache) { // 缓存查询的entities
					multiThreadExportService.cacheEntities.put(pageNumber, onePageData);
				}
			}
		} catch (Exception e) {
			System.out.println(new StringBuilder("=================================== query one page data failed !!!, pageNumber =").append(pageNumber).append(", pageSize =").append(pageSize));
			e.printStackTrace();
		}
	}

	/**
	 * 插入一行数据到sheet
	 * 
	 * @param oneRow
	 * @param rowIndex
	 */
	private void buildOneRow(List<String> oneRow, int rowIndex) {
		HSSFRow row = hssfSheet.createRow(rowIndex);
		HSSFCell cell = null;
		int columnIndex = 0;
		String column = null;
		for (Iterator<String> iter = oneRow.iterator(); iter.hasNext();) {
			column = iter.next();
			cell = row.createCell(columnIndex++);
			if (null == column || column.length() == 0) {
				column = " ";
			}
			cell.setCellValue(column);
		}
	}

	public Map<String, Object> buildPagenation(int pageSize, int pageNumber) {
		Map<String, Object> pagenation = new LinkedHashMap<String, Object>();
		pagenation.put("pageSize", pageSize);
		pagenation.put("pageNumber", pageNumber);
		return pagenation;
	}
}
