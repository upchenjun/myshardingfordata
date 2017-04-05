package com.fd.myshardingfordata.em;

/**
 * 条件类型
 * 
 * @author 符冬
 * 
 */
public enum PmType {
	/**
	 * 原生类型
	 * 
	 * createdate=moddate
	 */
	OG,
	/**
	 * 值类型
	 * 
	 * where amount !<100
	 */
	VL,
	/**
	 * 函数
	 * 
	 * having count(amount)>20
	 */
	FUN
}
