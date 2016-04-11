package yichen.massbatchexport;

import java.lang.reflect.ParameterizedType;

import org.junit.Test;

public class Base {

	@Test
	public void test1() {
		Dao<Base> ba = new Dao();
	}
}

class Dao<T> {

	Class<T> entityClazz; 
	
	public Dao() {
		
		ParameterizedType parameterizedType = (ParameterizedType)this.getClass().getGenericSuperclass(); 

		entityClazz= (Class<T>)(parameterizedType.getActualTypeArguments()[0]); 
		System.out.println(entityClazz);
	}
	
}