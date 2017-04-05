package com.fd.myshardingfordata.helper;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
/**
 * 事物管理器
 * @author 符冬
 *
 */
@Aspect
public class TransManager {
	@Around("@annotation(com.fd.myshardingfordata.annotation.MyTransaction)")
	public Object transactional(ProceedingJoinPoint pjp) throws Throwable {
		Object rz = null;
		try {
			connectionManager.beginTransaction();
			rz = pjp.proceed();
			connectionManager.commitTransaction();
		} catch (Throwable e) {
			connectionManager.rollbackTransaction();
			throw e;
		}
		return rz;
	}

	public void setConnectionManager(IConnectionManager connectionManager) {
		this.connectionManager = connectionManager;
	}

	public TransManager(IConnectionManager connectionManager) {
		super();
		this.connectionManager = connectionManager;
	}

	public TransManager() {
		super();
	}

	private IConnectionManager connectionManager;
}
