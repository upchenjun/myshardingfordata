package com.fd.myshardingfordata.helper;

import java.sql.Connection;

/**
 * 数据库连接管理
 * 
 * @author 符冬
 *
 */
public interface IConnectionManager {

	Connection getConnection();

	/**
	 * 开启事务
	 */
	void beginTransaction();

	/**
	 * 是否已经开启事务
	 * 
	 * @return
	 */
	boolean isTransactioning();

	/**
	 * 提交事务
	 */
	void commitTransaction();

	void closeConnection();

	/**
	 * 回滚事务
	 */
	void rollbackTransaction();

	/**
	 * 是否自动创建表和索引
	 * 
	 * @return
	 */
	boolean isGenerateDdl();

	Connection getWriteConnection();

	Connection getReadConnection();

	/**
	 *
	 * @param readOnly
	 *            是否只读
	 * @return
	 */
	Connection getConnection(boolean readOnly);

	/**
	 * 控制台打印SQL
	 * 
	 * @return
	 */
	boolean isShowSql();

}
