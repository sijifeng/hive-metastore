package com.kingnetdc.goldfish.hivemetastore.rest.model;

import java.sql.Timestamp;

/**
 * Created by jiyc on 2016/12/21.
 */
public class MetaStoreModel {
	private int id;
	private int p_id;
	private String type;
	private String db;
	private String catelog;
	private int date_type;
	private String structure;
	private String data;
	private Timestamp lastupdate;
	private long usetime;

	public long getUsetime() {
		return usetime;
	}

	public void setUsetime(long usetime) {
		this.usetime = usetime;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getCatelog() {
		return catelog;
	}

	public void setCatelog(String catelog) {
		this.catelog = catelog;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public int getDate_type() {
		return date_type;
	}

	public void setDate_type(int date_type) {
		this.date_type = date_type;
	}

	public String getDb() {
		return db;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public Timestamp getLastupdate() {
		return lastupdate;
	}

	public void setLastupdate(Timestamp lastupdate) {
		this.lastupdate = lastupdate;
	}

	public int getP_id() {
		return p_id;
	}

	public void setP_id(int p_id) {
		this.p_id = p_id;
	}

	public String getStructure() {
		return structure;
	}

	public void setStructure(String structure) {
		this.structure = structure;
	}
}
