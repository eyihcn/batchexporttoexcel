package yichen.massbatchexport;

public class Person {
	
	private String namge ;
	private Integer age ;
	private String password;
	
	
	public Person(String namge, Integer age, String password) {
		super();
		this.namge = namge;
		this.age = age;
		this.password = password;
	}
	public String getNamge() {
		return namge;
	}
	public void setNamge(String namge) {
		this.namge = namge;
	}
	public Integer getAge() {
		return age;
	}
	public void setAge(Integer age) {
		this.age = age;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	
	
}