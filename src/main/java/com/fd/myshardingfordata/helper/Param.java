package com.fd.myshardingfordata.helper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fd.myshardingfordata.em.Operate;
import com.fd.myshardingfordata.em.PmType;

/**
 * 查询条件参数
 * 
 * @author 符冬
 *
 */
public class Param {
	// 属性名称
	private String pname;
	// 操作
	private Operate operators;
	// between 第一个值
	private Object firstValue;
	// 值
	private Object value;
	// in查询条件的值
	private List<?> inValue;
	// 或者条件
	private Param orParam;
	// 条件类型
	private PmType cdType = PmType.VL;
	// 函数复合条件
	private String funName;

	public String getPname() {
		return pname;
	}

	public Param(String pname, Operate operators, Object value, String funName, PmType cdType) {
		super();
		this.pname = pname;
		this.operators = operators;
		this.value = value;
		this.funName = funName;
		this.cdType = cdType;
	}

	/**
	 * between 查询
	 * 
	 * @param pname
	 * @param firstValue
	 * @param value
	 */
	public Param(Object firstValue, String pname, Object value) {
		super();
		this.pname = pname;
		this.firstValue = firstValue;
		this.value = value;
		this.operators = Operate.BETWEEN;
	}

	/**
	 * 除了between
	 * 
	 * @param pname
	 * @param operators
	 * @param value
	 */
	public Param(String pname, Operate operators, Object value) {
		super();
		this.pname = pname;
		this.operators = operators;
		if (operators.equals(Operate.IN) || operators.equals(Operate.NOT_IN)) {
			this.inValue = (List<?>) value;
		} else {
			if (operators.equals(Operate.LIKE)) {
				this.value = "%" + value + "%";
			} else {
				this.value = value;
			}
		}
	}

	public Param(String pname, String value, boolean isleft) {
		this.pname = pname;
		this.operators = Operate.LIKE;
		if (isleft) {
			this.value = "%" + value;
		} else {
			this.value = value + "%";
		}
	}

	/**
	 * in 查询
	 * 
	 * @param pname
	 * @param inValue
	 */
	public Param(String pname, List<?> inValue) {
		super();
		this.pname = pname;
		this.inValue = inValue;
		this.operators = Operate.IN;
	}

	public Param() {
		super();
	}

	public void setPname(String pname) {
		this.pname = pname;
	}

	public Operate getOperators() {
		return operators;
	}

	public void setOperators(Operate operators) {
		this.operators = operators;
	}

	public Object getFirstValue() {
		return firstValue;
	}

	public void setFirstValue(Object firstValue) {
		this.firstValue = firstValue;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public List<?> getInValue() {
		return inValue;
	}

	public void setInValue(List<?> inValue) {
		this.inValue = inValue;
	}

	public Param getOrParam() {
		return orParam;
	}

	public void setOrParam(Param orParam) {
		this.orParam = orParam;
	}

	public PmType getCdType() {
		return cdType;
	}

	public void setCdType(PmType cdType) {
		this.cdType = cdType;
	}

	public String getFunName() {
		return funName;
	}

	public void setFunName(String funName) {
		this.funName = funName;
	}

	public static Set<Param> getParams(Param... params) {
		return new HashSet<>(Arrays.asList(params));
	}
}
