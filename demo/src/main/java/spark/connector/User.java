package spark.connector;

import java.io.Serializable;
import java.util.UUID;

public class User implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String id;
	private String user_name;
	private String unit;

	public User() {
		super();
	}

	public User(String id, String user_name, String unit) {
		super();
		this.id = id;
		this.user_name = user_name;
		this.unit = unit;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getUser_name() {
		return user_name;
	}

	public void setUser_name(String user_name) {
		this.user_name = user_name;
	}

	public String getUnit() {
		return unit;
	}

	public void setUnit(String unit) {
		this.unit = unit;
	}

	@Override
	public String toString() {
		return "User [id=" + id + ", user_name=" + user_name + ", unit=" + unit + "]";
	}

}
