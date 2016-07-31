package com.six_group.dgi.dsx.bigdata.poc;

import java.io.Serializable;

public class DeleteLeg implements Serializable {
	private static final long serialVersionUID = -3338252723406692293L;
	private String security;
	private long id;

	public DeleteLeg() {
	}

	public DeleteLeg(String security, long id) {
		super();
		this.security = security;
		this.id = id;
	}

	public String getSecurity() {
		return security;
	}

	public void setSecurity(String security) {
		this.security = security;
	}
	
	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

}
