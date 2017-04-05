package com.fd.myshardingfordata.dao.base.impl;

import javax.annotation.Resource;

import com.fd.myshardingfordata.helper.IConnectionManager;

public class DaoShardingBase<POJO> extends BaseShardingDao<POJO> {
	@Resource
	private IConnectionManager connectionManager;

	@Override
	protected IConnectionManager getConnectionManager() {
		return connectionManager;
	}


}
