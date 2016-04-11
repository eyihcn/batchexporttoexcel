package yichen.massbatchexport;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.junit.Test;


/**
 * Unit test for simple App.
 */
public class AppTest  {
	
	@Test
	public void test1() throws FileNotFoundException, IOException {
		long start = System.currentTimeMillis();
		FileOutputStream fileOutputStream = new FileOutputStream(new File("E:/aa.xls"));
		HSSFWorkbook workbook = new HSSFWorkbook();
		workbook = new HSSFWorkbook();
		HSSFSheet sheet = null;
		HSSFRow	row = null;
		HSSFCell cell = null;
		for (int sheetNum=0; sheetNum<5; sheetNum++) {
			sheet = workbook.createSheet();
			for (int rowIndex=1,len=PersonDao.list.size(); rowIndex<len; rowIndex++) {
				row = sheet.createRow(rowIndex);
				
				String column = PersonDao.list.get(rowIndex).getNamge();
				for (int columnIndex=0; columnIndex<3;columnIndex++) {
					cell= row.createCell(columnIndex);
					if(null == column || column.length()==0) {
						column = " ";
					}
					cell.setCellValue(column);
				}
			}
		}
		workbook.write(fileOutputStream);
		System.out.println((System.currentTimeMillis()-start)/1000 + "s ========");
	}
	
	/* totalPageSize = 20 ；pageSize = 4； rowSizeOfPersheet = 5；
	 * 					init
	 * sheetIndex		  -1    1    1   2   2   3   3   4  4 
	 * 		划分					4    1/  3   2/  2   3/  1  4	
	 * pageNumber		  0     1    2   2   3   3   4   4  5 
	 * 
	 * totalPageLeft 	  20	16   15  12  10  8   5   4  0
	 * taskCounts		  0     1    2   3   4   5   6   7  8
	 * prePageLeft        0     0    3   0   2   0   1   0  0
	 * 
	 * totalPageSize = 20 ；pageSize = 5； rowSizeOfPersheet = 4；
	 * 划分	               4 /1 3/ 2 2/ 3 1/4  
	 * totalPageLeft
	 * sheetIndex
	 * pageNumber
	 * taskCounts
	 */
	
	@Test
	public void test2() {
		long start = System.currentTimeMillis();
		int totalPageSize =PersonDao.list.size();
		// test 1
//		int pageSize = 4;
//		int rowSizeOfPersheet = 5;
		
		// test 1
		int pageSize = 250;
		int rowSizeOfPersheet = 500;
		//int pageSize = 1000;
		//int rowSizeOfPersheet = 10000;
		String[] headerRowData = {"name","age","password"};
		RowDataHandler<Person> rowDataHandler = new RowDataHandler<Person>() {
			
			public List<String> handler(Person entity) {
				String name = entity.getNamge();
				String age = entity.getAge().toString();
				return Arrays.asList(name,age,entity.getPassword());
			}
		};
		String exportFileName = "D:"+File.separator+"aa.xls";
		new MultiThreadExportService<Person, Object>(totalPageSize, headerRowData, new DataProvider<Person>() {

			public List<Person> providerOnePageDage(int pageSize, int pageNumber) {
				PersonDao p = new PersonDao();
				Map pageNation = new HashMap();
				pageNation.put("pageSize", pageSize);
				pageNation.put("pageNumber", pageNumber);
				return p.getPserons(null, null, pageNation);
			}
		}, rowDataHandler).setPageSize(pageSize).setRowSizeOfPersheet(rowSizeOfPersheet).export(exportFileName );
//		public MultiThreadExportService(int totalPageSize, String[] headerRowData,
//				String queryMethodName, Map<String, Object> queryParam,
//				Map<String, Object> sort, Class<D> daoClass,RowDataHandler<T> rowDataHandler) {
//		new MultiThreadExportService<Person, PersonDao>(totalPageSize, headerRowData, "getPserons", null, null, PersonDao.class, rowDataHandler).export("D:"+File.separator+"aa.xls");
		System.out.println((System.currentTimeMillis()-start)/1000 + "s ========");
	}
}
