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

	void beginTransaction();

	boolean isTransactioning();

	void commitTransaction();

	void closeConnection();

	void rollbackTransaction();

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
