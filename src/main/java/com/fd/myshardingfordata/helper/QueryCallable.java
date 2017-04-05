package com.fd.myshardingfordata.helper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.Callable;

/**
 * 并行查询
 * 
 * @author 符冬
 *
 */
public class QueryCallable implements Callable<ResultSet> {
	private PreparedStatement statement;

	@Override
	public ResultSet call() throws Exception {
		return statement.executeQuery();

	}

	public QueryCallable(PreparedStatement statement) {
		super();
		this.statement = statement;
	}

}
