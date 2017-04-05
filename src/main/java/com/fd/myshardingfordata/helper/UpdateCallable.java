package com.fd.myshardingfordata.helper;

import java.sql.PreparedStatement;
import java.util.concurrent.Callable;

/**
 * 并行更新
 * 
 * @author 符冬
 *
 */
public class UpdateCallable implements Callable<Integer> {
	private PreparedStatement statement;

	@Override
	public Integer call() throws Exception {
		return statement.executeUpdate();
	}

	public UpdateCallable(PreparedStatement statement) {
		super();
		this.statement = statement;
	}

}
