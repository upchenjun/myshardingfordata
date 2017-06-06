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
public class QueryCallable implements Callable<QueryVo<ResultSet>> {
	private PreparedStatement statement;
	private String tbn;

	@Override
	public QueryVo<ResultSet> call() throws Exception {
		ResultSet rs = statement.executeQuery();
		return new QueryVo<ResultSet>(tbn, rs);

	}

	public QueryCallable(PreparedStatement statement, String tbn) {
		super();
		this.statement = statement;
		this.tbn = tbn;
	}

	public QueryCallable() {
		super();
	}

	public QueryCallable(PreparedStatement statement) {
		super();
		this.statement = statement;
	}

}
