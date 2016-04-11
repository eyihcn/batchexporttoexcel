package yichen.massbatchexport;
import java.util.List;

/**
 * 
 * @author chenyi
 *
 * @param <T>
 */
public interface DataProvider<T, D> {
	
	/**
	 * 提供一页的数据
	 * 
	 * @param pageSize
	 *            一页的数据量
	 * @param pageNumber
	 *            页码：1 2 3 ...
	 * @return
	 */
	List<T> providerOnePageDage(D dao, int pageSize, int pageNumber);

}
