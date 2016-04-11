package yichen.massbatchexport;

import java.util.List;

/**
 * 
 * @author chenyi
 *
 * @param <T>
 */
public interface RowDataHandler<T> {

	/**
	 * 具体将实体转换为一行数据
	 * @param entity
	 * @return
	 */
	List<String> handler(T entity) ;
		
}
