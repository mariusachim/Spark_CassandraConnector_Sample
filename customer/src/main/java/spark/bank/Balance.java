package spark.bank;

import java.io.Serializable;

public class Balance implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String customerid;

	private String card;

	private Integer balance;

	private String update_at;

	public Balance() {
		super();
	}

	public Balance(String customerid, String card, Integer balance, String update_at) {
		super();
		this.customerid = customerid;
		this.card = card;
		this.balance = balance;
		this.update_at = update_at;
	}

	public String getCustomerid() {
		return customerid;
	}

	public void setCustomerid(String customerid) {
		this.customerid = customerid;
	}

	public String getCard() {
		return card;
	}

	public void setCard(String card) {
		this.card = card;
	}

	public Integer getBalance() {
		return balance;
	}

	public void setBalance(Integer balance) {
		this.balance = balance;
	}

	public String getUpdate_at() {
		return update_at;
	}

	public void setUpdate_at(String update_at) {
		this.update_at = update_at;
	}

	@Override
	public String toString() {
		return "Balance [customerid=" + customerid + ", card=" + card + ", balance=" + balance + ", update_at="
				+ update_at + "]";
	}

}
