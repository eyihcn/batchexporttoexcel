package yichen.massbatchexport;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PersonDao {
	static int size = 2220;
//	static int size = 100000;
	static List<Person> list = new ArrayList<Person>(size);
	
	static {
		for (int index=0; index<size; index++) {
			list.add(new Person(index+"", index, index+""));
		}
	}
	
	public List<Person> getPserons(Map queryParam, Map sort,Map pageNation) {
		int pageSize = (Integer) pageNation.get("pageSize");
		int pageNum = (Integer) pageNation.get("pageNumber");
		return list.subList((pageNum-1)*pageSize>=list.size()?list.size()-1:(pageNum-1)*pageSize
				, pageNum*pageSize>=list.size()?list.size()-1:pageNum*pageSize);
	}
	
}

