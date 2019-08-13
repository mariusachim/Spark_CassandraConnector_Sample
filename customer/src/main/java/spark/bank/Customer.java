package spark.bank;

import java.io.Serializable;

public class Customer implements Serializable {
	
	public static final String TABLE_NAME = "cc_customer";
	
	private String id;

	private String county;

	private String name;

	public Customer() {
		super();
	}

	public Customer(String id, String county, String name) {
		super();
		this.id = id;
		this.county = county;
		this.name = name;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getCounty() {
		return county;
	}

	public void setCounty(String county) {
		this.county = county;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
