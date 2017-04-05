package com.fd.myshardingfordata.helper;

import java.util.Arrays;
import java.util.LinkedHashSet;
/**
 * 排序属性信息
 * @author 符冬
 *
 */
public class SortInfo {
	private String pname;
	private boolean isDesc;
	
	public String getPname() {
		return pname;
	}

	public void setPname(String pname) {
		this.pname = pname;
	}

	public boolean isDesc() {
		return isDesc;
	}

	public void setDesc(boolean isDesc) {
		this.isDesc = isDesc;
	}

	public SortInfo(String pname, boolean isDesc) {
		super();
		this.pname = pname;
		this.isDesc = isDesc;
	}

	public SortInfo() {
		super();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (isDesc ? 1231 : 1237);
		result = prime * result + ((pname == null) ? 0 : pname.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SortInfo other = (SortInfo) obj;
		if (isDesc != other.isDesc)
			return false;
		if (pname == null) {
			if (other.pname != null)
				return false;
		} else if (!pname.equals(other.pname))
			return false;
		return true;
	}

	public static LinkedHashSet<SortInfo> getSortInfos(SortInfo... infos) {
		return new LinkedHashSet<>(Arrays.asList(infos));
	}

}
