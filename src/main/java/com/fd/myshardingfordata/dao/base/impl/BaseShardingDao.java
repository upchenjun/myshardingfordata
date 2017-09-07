package com.fd.myshardingfordata.dao.base.impl;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

import javax.annotation.PostConstruct;
import javax.persistence.Column;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fd.myshardingfordata.annotation.ColumnRule;
import com.fd.myshardingfordata.dao.base.IBaseShardingDao;
import com.fd.myshardingfordata.em.KSentences;
import com.fd.myshardingfordata.em.Operate;
import com.fd.myshardingfordata.em.PmType;
import com.fd.myshardingfordata.em.RuleType;
import com.fd.myshardingfordata.em.StatisticsType;
import com.fd.myshardingfordata.helper.ConnectionManager;
import com.fd.myshardingfordata.helper.IConnectionManager;
import com.fd.myshardingfordata.helper.MyObjectUtils;
import com.fd.myshardingfordata.helper.ObData;
import com.fd.myshardingfordata.helper.PageData;
import com.fd.myshardingfordata.helper.Param;
import com.fd.myshardingfordata.helper.PropInfo;
import com.fd.myshardingfordata.helper.QueryCallable;
import com.fd.myshardingfordata.helper.QueryVo;
import com.fd.myshardingfordata.helper.SortComparator;
import com.fd.myshardingfordata.helper.SortInfo;
import com.fd.myshardingfordata.helper.UpdateCallable;

public abstract class BaseShardingDao<POJO> implements IBaseShardingDao<POJO> {
	private static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	@Override
	public Integer deleteById(Serializable... id) {
		if (id != null && id.length > 0) {
			Set<Param> pms = Param.getParams(new Param(getPrimaryKeyPname(), Arrays.asList(id)));
			return deleteByCondition(pms);
		}
		return 0;
	}

	@Override
	public Integer update(Set<Param> pms, Map<String, Object> newValues) {
		if (getCurrentTables().size() < 1) {
			return 0;
		}
		try {
			Set<String> tbns = getTableNamesByParams(pms);
			if (newValues != null && newValues.size() > 0) {
				List<PreparedStatement> pss = new ArrayList<>();
				Set<PropInfo> pps = getPropInfos();
				for (String tn : tbns) {
					StringBuilder buf = new StringBuilder(KSentences.UPDATE.getValue());
					buf.append(tn).append(KSentences.SET.getValue());
					Iterator<Entry<String, Object>> ite = newValues.entrySet().iterator();
					while (ite.hasNext()) {
						Entry<String, Object> en = ite.next();
						for (PropInfo p : pps) {
							if (p.getPname().equals(en.getKey())) {
								buf.append(p.getCname()).append(KSentences.EQ.getValue())
										.append(KSentences.POSITION_PLACEHOLDER.getValue());
								if (ite.hasNext()) {
									buf.append(KSentences.COMMA.getValue());
								}
							}
						}
					}
					buf.append(getWhereSqlByParam(pms));
					String sql = buf.toString();
					PreparedStatement statement = getStatementBySql(false, sql);
					if (getConnectionManager().isShowSql()) {
						log.info(sql);
					}
					setWhereSqlParamValue(pms, statement, setUpdateNewValues(newValues, statement));
					if (tbns.size() == 1) {
						return statement.executeUpdate();
					} else {
						pss.add(statement);
					}
				}
				return executeUpdate(pss);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		} finally {
			getConnectionManager().closeConnection();

		}
		return 0;
	}

	@Override
	public Integer delete(Set<Param> pms) {
		if (getCurrentTables().size() < 1) {
			return 0;
		}
		return deleteByCondition(pms);
	}

	@Override
	public Double getStatisticsValue(Set<Param> pms, String property, StatisticsType functionName) {
		double ttc = 0;
		if (property != null && functionName != null) {
			if (getCurrentTables().size() < 1) {
				return 0d;
			}
			StringBuffer sb = new StringBuffer(KSentences.SELECT.getValue());
			for (PropInfo p : getPropInfos()) {
				if (p.getPname().equals(property.trim())) {
					sb.append(functionName).append(KSentences.LEFT_BRACKETS.getValue()).append(p.getCname())
							.append(KSentences.RIGHT_BRACKETS.getValue()).append(KSentences.FROM);
					break;
				}
			}
			try {
				List<Double> rzslist = new ArrayList<>();
				List<Future<QueryVo<ResultSet>>> rzts = invokeall(true, pms, sb.toString());
				for (Future<QueryVo<ResultSet>> f : rzts) {
					ResultSet rs = f.get().getOv();
					while (rs.next()) {
						Object o = rs.getObject(1);
						if (o != null) {
							rzslist.add(Double.valueOf(o.toString()));
						}
					}

				}
				if (rzslist.size() > 0) {
					if (StatisticsType.MAX.equals(functionName)) {
						return rzslist.parallelStream().max(Comparator.comparing(d -> d)).get();
					} else if (StatisticsType.MIN.equals(functionName)) {
						return rzslist.parallelStream().min(Comparator.comparing(d -> d)).get();

					} else if (StatisticsType.SUM.equals(functionName)) {
						return rzslist.parallelStream().mapToDouble(i -> i).sum();
					}
				}
			} catch (Exception e) {
				throw new IllegalStateException(e);
			} finally {
				getConnectionManager().closeConnection();
			}
		}
		return ttc;
	}

	@Override
	public Long getCount(Set<Param> pms) {
		return getttc(true, pms);
	}

	@Override
	public Long getCountFromMaster(Set<Param> pms) {
		return getttc(false, pms);
	}

	private Long getttc(boolean isRead, Set<Param> pms) {
		if (getCurrentTables().size() < 1) {
			return 0L;
		}
		List<QueryVo<Long>> qvs = getCountPerTable(isRead, pms);
		return getQvcSum(qvs);

	}

	private Long getQvcSum(List<QueryVo<Long>> qvs) {
		if (qvs != null && qvs.size() > 0) {
			return qvs.stream().filter(o -> o.getOv() != null).mapToLong(QueryVo::getOv).sum();
		} else {
			return 0L;
		}
	}

	private List<Future<QueryVo<ResultSet>>> invokeall(boolean isRead, Set<Param> pms, String sqlselect)
			throws SQLException {
		Iterator<String> tbnsite = getTableNamesByParams(pms).iterator();
		List<QueryVo<PreparedStatement>> pss = new ArrayList<>();
		String whereSqlByParam = getWhereSqlByParam(pms);
		while (tbnsite.hasNext()) {
			String tn = tbnsite.next();
			String sql = sqlselect + tn + whereSqlByParam;
			PreparedStatement statement = getStatementBySql(isRead, sql);
			setWhereSqlParamValue(pms, statement);
			pss.add(new QueryVo<PreparedStatement>(tn, statement));
		}
		return invokeQueryAll(pss);
	}

	@Override
	public List<POJO> getList(Set<Param> pms, String... cls) {
		return getRztPos(true, pms, cls);
	}

	@Override
	public List<POJO> getListFromMater(Set<Param> pms, String... cls) {
		return getRztPos(false, pms, cls);
	}

	@Override
	public List<POJO> getListAndOrderBy(LinkedHashSet<ObData> orderbys, Set<Param> pms, String... cls) {
		if (getCurrentTables().size() < 1) {
			return new ArrayList<>(0);
		}
		return getRztPos(true, 1, Integer.MAX_VALUE / getCurrentTables().size(), orderbys, pms, cls);
	}

	@Override
	public List<POJO> getList(int curPage, int pageSize, LinkedHashSet<ObData> orderbys, Set<Param> pms,
			String... cls) {
		return getRztPos(true, curPage, pageSize, orderbys, pms, cls);
	}

	@Override
	public List<POJO> getListFromMaster(int curPage, int pageSize, LinkedHashSet<ObData> orderbys, Set<Param> pms,
			String... cls) {
		return getRztPos(false, curPage, pageSize, orderbys, pms, cls);
	}

	@Override
	public PageData<POJO> getPageInfoFromMaster(Set<Param> pms, int curPage, int pageSize, String... cls) {
		return getListFromNotSorted(false, curPage, pageSize, pms, cls);
	}

	@Override
	public PageData<POJO> getPageInfo(Set<Param> pms, int curPage, int pageSize, String... cls) {
		return getListFromNotSorted(true, curPage, pageSize, pms, cls);
	}

	@Override
	public List<POJO> getListFromMaster(Set<Param> pms, int curPage, int pageSize, String... cls) {
		return getListFromNotSorted(false, curPage, pageSize, pms, cls).getDataList();
	}

	@Override
	public List<POJO> getList(Set<Param> pms, int curPage, int pageSize, String... cls) {
		return getListFromNotSorted(true, curPage, pageSize, pms, cls).getDataList();
	}

	@Override
	public PageData<Object[]> getGroupPageInfo(int curPage, int pageSize, LinkedHashSet<ObData> orderbys,
			Set<Param> pms, LinkedHashMap<String, String> funs, String... groupby) {
		if (pms == null) {
			pms = new HashSet<>();
		}
		return new PageData<>(curPage, pageSize, getGroupbyCount(new HashSet<>(pms), groupby),
				getGroupList(curPage, pageSize, orderbys, pms, funs, groupby));
	}

	@Override
	public Long getGroupbyCount(Set<Param> pms, String... groupby) {
		return groupcount(pms, groupby);

	}

	private Set<String> getobfp(Set<Param> pms) {
		if (pms != null) {
			Set<String> pps = new HashSet<>(pms.size());
			for (Param p : pms) {
				if (p.getFunName() != null && PmType.FUN.equals(p.getCdType())) {
					pps.add(p.getPname());
					while (p.getOrParam() != null) {
						p = p.getOrParam();
						if (p.getFunName() != null && PmType.FUN.equals(p.getCdType())) {
							pps.add(p.getPname());
						}
					}
				}
			}
		}
		return Collections.emptySet();

	}

	private Long groupcount(Set<Param> pms, String... groupby) {
		if (groupby == null || groupby.length == 0 || getCurrentTables().size() < 1) {
			return 0L;
		}
		try {
			if (pms != null) {
				pms = new HashSet<>(pms);
			}
			Set<String> getobfp = getobfp(pms);
			Set<String> tbns = getTableNamesByParams(pms);
			Set<Param> hvcs = gethvconditions(pms);
			String whereSqlByParam = getWhereSqlByParam(pms);

			StringBuilder sqlsb = new StringBuilder("SELECT COUNT(1) FROM  (SELECT count(");
			for (PropInfo prop : getPropInfos()) {
				if (prop.getPname().equals(groupby[0].trim())) {
					sqlsb.append(prop.getCname());
					break;
				}
			}
			sqlsb.append(")  FROM (");
			Iterator<String> tnite = tbns.iterator();
			while (tnite.hasNext()) {
				String tn = tnite.next();
				sqlsb.append(getPreSelectSql(getGSelect(groupby, getobfp))).append(tn).append(whereSqlByParam);
				if (tnite.hasNext()) {
					sqlsb.append(KSentences.UNION_ALL.getValue());
				}
			}
			String havingSql = getHavingSql(hvcs);
			sqlsb.append(")  gdtc  ").append(KSentences.GROUPBY.getValue()).append(groupbysql(groupby))
					.append(havingSql).append(")  ccfd ");
			String sql = sqlsb.toString();
			PreparedStatement statement = getStatementBySql(true, sql);
			if (getConnectionManager().isShowSql()) {
				log.info(sql);
			}
			int ix = 1;
			for (String tn : tbns) {
				ix = setWhereSqlParamValue(pms, statement, ix);
			}
			setWhereSqlParamValue(hvcs, statement, ix);
			ResultSet rs = statement.executeQuery();
			if (rs.next()) {
				Number nm = (Number) rs.getObject(1);
				return nm.longValue();
			}

			return 0L;
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		} finally {
			getConnectionManager().closeConnection();
		}
	}

	private String groupbysql(String[] groupby) {

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < groupby.length; i++) {
			String g = groupby[i];
			for (PropInfo p : getPropInfos()) {
				if (p.getPname().equals(g)) {
					sb.append(p.getCname());
				}
			}
			if (i < groupby.length - 1) {
				sb.append(KSentences.COMMA.getValue());
			}
		}
		return sb.toString();
	}

	@Override
	public List<Object[]> getGroupList(int curPage, int pageSize, LinkedHashSet<ObData> orderbys, Set<Param> pms,
			LinkedHashMap<String, String> funs, String... groupby) {

		return grouplist(true, curPage, pageSize, orderbys, pms, funs, groupby);
	}

	@Override
	public List<Object[]> getGroupListFromMaster(int curPage, int pageSize, LinkedHashSet<ObData> orderbys,
			Set<Param> pms, LinkedHashMap<String, String> funs, String... groupby) {

		return grouplist(false, curPage, pageSize, orderbys, pms, funs, groupby);
	}

	private List<Object[]> grouplist(boolean readOnly, int curPage, int pageSize, LinkedHashSet<ObData> orderbys,
			Set<Param> pms, LinkedHashMap<String, String> funs, String... groupby) {
		try {
			if (curPage < 1 || pageSize < 1 || groupby == null || groupby.length == 0
					|| getCurrentTables().size() < 1) {
				return new ArrayList<>(0);
			}
			if (pms != null) {
				pms = new HashSet<>(pms);
			}
			/**
			 * 分组函数条件
			 */
			Set<Param> hvcs = gethvconditions(pms);
			/**
			 * where 条件
			 */
			String whereSqlByParam = getWhereSqlByParam(pms);

			StringBuilder grpsql = new StringBuilder(KSentences.SELECT.getValue());
			/**
			 * 拼接查询函数
			 */
			if (funs != null && funs.size() > 0) {
				Iterator<Entry<String, String>> enite = funs.entrySet().iterator();
				while (enite.hasNext()) {
					Entry<String, String> funen = enite.next();
					for (PropInfo p : getPropInfos()) {
						if (p.getPname().equals(funen.getValue())) {
							grpsql.append(funen.getKey().trim().toUpperCase()).append("(").append(p.getCname().trim())
									.append(")").append(KSentences.COMMA.getValue());
							break;
						}
					}
				}

			}
			/**
			 * 拼接查询字段
			 */
			for (int i = 0; i < groupby.length; i++) {
				for (PropInfo p : getPropInfos()) {
					if (groupby[i].equals(p.getPname())) {
						grpsql.append(p.getCname());
						break;
					}
				}

				if (i < groupby.length - 1) {
					grpsql.append(KSentences.COMMA.getValue());
				}
			}

			grpsql.append(KSentences.FROM.getValue()).append("(");
			/**
			 * 汇总所有表数据
			 */
			Set<String> tbns = getTableNamesByParams(pms);
			/**
			 * select groupby from
			 */
			String selectpre = getPreSelectSql(getGSelect(groupby, funs != null ? funs.values() : null));
			Iterator<String> tnite = tbns.iterator();
			while (tnite.hasNext()) {
				String tn = tnite.next();
				grpsql.append(selectpre).append(tn).append(whereSqlByParam);
				if (tnite.hasNext()) {
					grpsql.append(KSentences.UNION_ALL.getValue());
				}
			}
			grpsql.append(")  gdt ").append(KSentences.GROUPBY.getValue()).append(groupbysql(groupby))
					.append(getHavingSql(hvcs));
			if (orderbys != null && orderbys.size() > 0) {
				grpsql.append(KSentences.ORDERBY.getValue());
				Iterator<ObData> obite = orderbys.iterator();
				c: while (obite.hasNext()) {
					ObData ob = obite.next();
					if (ob.getFunName() != null) {
						Set<Entry<String, String>> ens = funs.entrySet();
						for (Entry<String, String> en : ens) {
							if (en.getValue().equals(ob.getPropertyName())) {
								grpsql.append(en.getKey().trim().toUpperCase()).append("(").append(en.getValue().trim())
										.append(")");

							}
						}
					} else {
						a: for (PropInfo p : getPropInfos()) {
							if (p.getPname().equals(ob.getPropertyName())) {
								for (String g : groupby) {
									if (g.trim().equals(p.getPname())) {
										grpsql.append(p.getCname().trim());
										break a;
									}
								}
								continue c;
							}
						}

					}
					if (ob.getIsDesc()) {
						grpsql.append(KSentences.DESC.getValue());
					}
					if (obite.hasNext()) {
						grpsql.append(KSentences.COMMA.getValue());
					}

				}
				if (grpsql.lastIndexOf(KSentences.COMMA.getValue()) == grpsql.length() - 1) {
					grpsql.deleteCharAt(grpsql.length() - 1);
				}

			}
			String selectPagingSql = getSingleTableSelectPagingSql(grpsql.toString(), curPage, pageSize);
			PreparedStatement statement = getStatementBySql(readOnly, selectPagingSql);
			if (getConnectionManager().isShowSql()) {
				log.info(selectPagingSql);
			}
			int ix = 1;
			for (String tn : tbns) {
				ix = setWhereSqlParamValue(pms, statement, ix);
			}
			setWhereSqlParamValue(hvcs, statement, ix);
			return getObjectList(statement.executeQuery());
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		} finally {
			getConnectionManager().closeConnection();
		}
	}

	private PreparedStatement getStatementBySql(boolean readOnly, String selectPagingSql) throws SQLException {
		PreparedStatement statement = getConnectionManager().getConnection(readOnly).prepareStatement(selectPagingSql);
		// 300秒超时
		statement.setQueryTimeout(360);
		return statement;
	}

	private String getHavingSql(Set<Param> hvcs) {
		if (hvcs.size() > 0) {
			StringBuilder sb = new StringBuilder(KSentences.HAVING.getValue());
			geneConditionSql(hvcs, sb);
			return sb.toString();
		}
		return "";
	}

	private Set<Param> gethvconditions(Set<Param> pms) {
		Set<Param> hvcs = new HashSet<>();
		if (pms != null && pms.size() > 0) {
			Iterator<Param> pmite = pms.iterator();
			while (pmite.hasNext()) {
				Param pm = pmite.next();
				if (pm.getFunName() != null && pm.getFunName().length() > 0) {
					for (PropInfo p : getPropInfos()) {
						if (p.getPname().equals(pm.getPname())) {
							hvcs.add(pm);
							pmite.remove();
						}
					}
				}
			}
		}
		return hvcs;
	}

	@Override
	public List<POJO> getListAndOrderBy(LinkedHashSet<ObData> orderbys, String... cls) {
		if (getCurrentTables().size() < 1) {
			return new ArrayList<>(0);
		}
		return getRztPos(true, 1, Integer.MAX_VALUE / getCurrentTables().size(), orderbys, null, cls);
	}

	@Override
	public List<POJO> getListByIds(List<Serializable> ids, String... strings) {
		if (ids != null && ids.size() > 0) {
			Set<PropInfo> pis = getPropInfos();
			for (PropInfo fd : pis) {
				if (fd.getIsPrimarykey()) {
					return getRztPos(true, Param.getParams(new Param(fd.getPname(), ids)), strings);
				}
			}

		}
		return new ArrayList<>(0);
	}

	@Override
	public List<POJO> getList(String propertyName, List<Serializable> vls, String... cls) {
		if (vls != null && vls.size() > 0) {
			Set<PropInfo> pis = getPropInfos();
			for (PropInfo fd : pis) {
				if (fd.getPname().equals(propertyName)) {
					return getRztPos(true, Param.getParams(new Param(fd.getPname(), vls)), cls);
				}
			}
		}

		return new ArrayList<>(0);
	}

	// 获取主键名称
	private String getPrimaryKeyPname() {
		for (PropInfo fd : getPropInfos()) {
			if (fd.getIsPrimarykey()) {
				return fd.getPname();
			}
		}
		throw new IllegalStateException(
				String.format("%s没有定义主键！！", ConnectionManager.getTbinfo(clazz).entrySet().iterator().next().getKey()));
	}

	@Override
	public POJO getById(Serializable id, String... strings) {
		return getObjByid(true, id, strings);

	}

	@Override
	public POJO getByIdFromMaster(Serializable id, String... strings) {
		return getObjByid(false, id, strings);

	}

	protected POJO getObjByid(Boolean isRead, Serializable id, String... strings) {
		if (id != null) {
			try {
				Entry<String, LinkedHashSet<PropInfo>> tbimp = ConnectionManager.getTbinfo(clazz).entrySet().iterator()
						.next();
				for (PropInfo fd : tbimp.getValue()) {
					if (fd.getIsPrimarykey()) {
						ColumnRule cr = fd.getColumnRule();
						Set<Param> pms = Param.getParams(new Param(fd.getPname(), Operate.EQ, id));
						if (cr != null) {
							List<POJO> rzlist = getSingleObj(isRead, id, tbimp, fd, cr, pms, strings);
							if (rzlist.size() == 1) {
								return rzlist.get(0);
							}
						} else {
							List<POJO> rzlist = getRztPos(isRead, pms, strings);
							if (rzlist.size() == 1) {
								return rzlist.get(0);
							}
						}
						break;
					}
				}

			} catch (Exception e) {
				e.printStackTrace();
				throw new IllegalStateException(e);
			} finally {
				getConnectionManager().closeConnection();
			}
		}
		return null;
	}

	private List<POJO> getSingleObj(Boolean isRead, Serializable id, Entry<String, LinkedHashSet<PropInfo>> tbimp,
			PropInfo fd, ColumnRule cr, Set<Param> pms, String... strings) throws SQLException {
		String tableName = getTableName(getTableMaxIdx(id, fd.getType(), cr), tbimp.getKey());
		if (!isContainsTable(tableName)) {
			return new ArrayList<>(0);
		}
		StringBuilder sb = getSelectSql(tableName, strings);
		sb.append(getWhereSqlByParam(pms));
		String sql = sb.toString();
		PreparedStatement prepare = getStatementBySql(isRead, sql);
		if (getConnectionManager().isShowSql()) {
			log.info(sql);
		}
		setWhereSqlParamValue(pms, prepare);
		ResultSet rs = prepare.executeQuery();
		return getRztObject(rs, strings);
	}

	@Override
	public POJO get(String propertyName, Serializable value, String... cls) {
		return getObj(true, propertyName, value, cls);
	}

	@Override
	public POJO getByMaster(String propertyName, Serializable value, String... cls) {
		return getObj(false, propertyName, value, cls);
	}

	private POJO getObj(Boolean isRead, String propertyName, Serializable value, String... cls) {
		try {
			Entry<String, LinkedHashSet<PropInfo>> tbimp = ConnectionManager.getTbinfo(clazz).entrySet().iterator()
					.next();
			for (PropInfo fd : tbimp.getValue()) {
				if (fd.getPname().equals(propertyName)) {
					Set<Param> pms = Param.getParams(new Param(fd.getPname(), Operate.EQ, value));
					if (value != null && fd.getColumnRule() != null) {

						List<POJO> rzlist = getSingleObj(isRead, value, tbimp, fd, fd.getColumnRule(), pms, cls);
						if (rzlist.size() == 1) {
							return rzlist.get(0);
						}
					} else {
						List<POJO> rzlist = getRztPos(isRead, pms, cls);
						if (rzlist.size() == 1) {
							return rzlist.get(0);
						}
					}
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		} finally {
			getConnectionManager().closeConnection();
		}

		return null;
	}

	@Override
	public POJO get(Set<Param> pms, String... cls) {
		List<POJO> rzlist = getRztPos(true, pms, cls);
		if (rzlist.size() == 1) {
			return rzlist.get(0);
		}

		return null;
	}

	@Override
	public Integer saveList(List<POJO> pojos) {
		int i = 0;
		if (pojos != null) {
			boolean istransaction = getConnectionManager().isTransactioning();
			try {
				if (!istransaction) {
					getConnectionManager().beginTransaction();
				}
				for (POJO po : pojos) {
					i += persist(po);
				}
				if (!istransaction) {
					getConnectionManager().commitTransaction();
				}
			} catch (Throwable e) {
				if (!istransaction) {
					getConnectionManager().rollbackTransaction();
				}
				e.printStackTrace();
				throw new IllegalArgumentException(e);
			} finally {
				getConnectionManager().closeConnection();
			}
		}
		return i;
	}

	@Override
	public Integer save(POJO pojo) {
		int rzc = 0;
		if (pojo != null) {

			try {
				rzc = persist(pojo);
			} catch (Exception e) {
				e.printStackTrace();
				throw new IllegalArgumentException(e);
			} finally {
				getConnectionManager().closeConnection();
			}
		}
		return rzc;
	}

	protected int persist(POJO pojo) throws IllegalAccessException, SQLException {
		Field[] fds = clazz.getDeclaredFields();
		Entry<String, LinkedHashSet<PropInfo>> tbe = ConnectionManager.getTbinfo(clazz).entrySet().iterator().next();
		Field idkey = checkPrimarykey(fds, tbe);

		StringBuilder sb = new StringBuilder(KSentences.INSERT.getValue());
		sb.append(tableSharding(pojo, fds, tbe.getKey()));
		sb.append("(");
		Iterator<PropInfo> clite = tbe.getValue().iterator();
		while (clite.hasNext()) {
			sb.append(clite.next().getCname());
			if (clite.hasNext()) {
				sb.append(KSentences.COMMA.getValue());
			}
		}
		sb.append(")  VALUES(");
		for (int i = 0; i < tbe.getValue().size(); i++) {
			sb.append(KSentences.POSITION_PLACEHOLDER.getValue());
			if (i < tbe.getValue().size() - 1) {
				sb.append(KSentences.COMMA.getValue());
			}
		}
		sb.append(")");

		PreparedStatement statement = getConnectionManager().getConnection().prepareStatement(sb.toString(),
				Statement.RETURN_GENERATED_KEYS);
		setParamVal(pojo, fds, tbe.getValue(), statement);
		int cc = statement.executeUpdate();
		ResultSet rs = statement.getGeneratedKeys();
		if (rs.next()) {
			idkey.setAccessible(true);
			idkey.set(pojo, rs.getLong(1));
		}
		return cc;
	}

	/**
	 * 表切分
	 * 
	 * @param pojo
	 * @param fds
	 * @param name
	 * @param sb
	 * @throws IllegalAccessException
	 * @throws SQLException
	 */
	private String tableSharding(POJO pojo, Field[] fds, String name) throws IllegalAccessException, SQLException {
		for (Field fd : fds) {
			ColumnRule crn = fd.getAnnotation(ColumnRule.class);
			if (crn != null) {
				fd.setAccessible(true);
				if (fd.get(pojo) == null) {
					throw new IllegalArgumentException(String.format("%s切分字段数据不能为空！！", fd.getName()));
				}
				long max = getTableMaxIdx(fd.get(pojo), fd.getType(), crn);
				if (getCurrentTables().size() >= maxTableCount) {
					throw new IllegalStateException(String.format("超出了表拆分最大数量，最多只能拆分%s个表", maxTableCount));
				}
				String ctbname = getTableName(max, name);
				if (!isExistTable(ctbname)) {
					synchronized (FIRST_TABLECREATE) {
						reFreshTables();
						if (!isExistTable(ctbname)) {
							String dpname = getConnectionManager().getConnection().getMetaData()
									.getDatabaseProductName();
							if ("MySQL".equalsIgnoreCase(dpname)) {
								String sql = KSentences.CREATE_TABLE.getValue() + ctbname + KSentences.LIKE + name;
								getConnectionManager().getConnection().prepareStatement(sql).executeUpdate();
								if (getConnectionManager().isShowSql()) {
									log.info(sql);
								}
								getCurrentTables().add(ctbname);
							}

						}
					}
				}
				return ctbname;
			}
		}

		return name;
	}

	private Field checkPrimarykey(Field[] fds, Entry<String, LinkedHashSet<PropInfo>> tbe) {
		Field idkey = null;
		for (Field fd : fds) {
			if (fd.isAnnotationPresent(Id.class)) {
				idkey = fd;
				break;
			}
		}
		if (idkey == null) {
			throw new IllegalStateException(String.format("%s没有定义主键！！", tbe.getKey()));
		}
		return idkey;
	}

	private final static Object FIRST_TABLECREATE = new Object();

	private int setUpdateNewValues(Map<String, Object> newValues, PreparedStatement statement) throws SQLException {
		Iterator<Entry<String, Object>> ite = newValues.entrySet().iterator();
		int i = 1;
		while (ite.hasNext()) {
			Entry<String, Object> next = ite.next();
			statement.setObject(i++, getParamSqlValue(next.getValue(), next.getKey()));
		}
		return i;
	}

	/**
	 * 表的下标
	 * 
	 * @param v
	 * @param fd
	 * @param crn
	 * @return
	 */
	private long getTableMaxIdx(Object v, Class<?> type, ColumnRule crn) {
		long max = 0;
		if (type == Long.class) {
			max = getTbIdx(Long.valueOf(v.toString()), crn);
		} else if (type == Integer.class) {
			if (crn.ruleType().equals(RuleType.RANGE)) {
				max = Integer.valueOf(v.toString()) / crn.value();
			} else {
				max = Integer.valueOf(v.toString()) % crn.value();
			}
		} else if (type == String.class) {

			if (crn.ruleType().equals(RuleType.RANGE)) {
				max = Math.abs(v.toString().hashCode()) / crn.value();
			} else {
				max = Math.abs(v.toString().hashCode()) % crn.value();
			}

		} else if (type == Date.class) {
			Date dt = (Date) v;
			if (v.getClass() != type) {
				dt = new Date(dt.getTime());
			}
			max = getTbIdx(dt.toInstant().atZone(ZoneId.systemDefault()).toLocalDate().toEpochDay(), crn);

		} else if (type == Timestamp.class) {

			Timestamp dt = (Timestamp) v;
			max = getTbIdx(dt.toLocalDateTime().toLocalDate().toEpochDay(), crn);

		} else if (type == LocalDate.class) {

			LocalDate dt = (LocalDate) v;
			max = getTbIdx(dt.toEpochDay(), crn);

		} else if (type == LocalDateTime.class) {

			LocalDateTime dt = (LocalDateTime) v;
			max = getTbIdx(dt.toLocalDate().toEpochDay(), crn);

		} else {
			throw new IllegalStateException(String.format("%s类型不能用来对数据进行切分，请使用int、long、string、date类型的字段", type));
		}
		return max;
	}

	private static long getTbIdx(long tv, ColumnRule crn) {

		if (crn.ruleType().equals(RuleType.RANGE)) {
			return tv / crn.value();
		} else {
			return tv % crn.value();
		}
	}

	private int deleteByCondition(Set<Param> pms) {
		if (getCurrentTables().size() < 1) {
			return 0;
		}
		try {
			Set<String> tbns = getTableNamesByParams(pms);
			List<PreparedStatement> pss = new ArrayList<>();
			String whereSqlByParam = getWhereSqlByParam(pms);
			for (String tn : tbns) {
				String sql = KSentences.DELETE_FROM.getValue() + tn + whereSqlByParam;
				PreparedStatement statement = getStatementBySql(false, sql);
				if (getConnectionManager().isShowSql()) {
					log.info(sql);
				}
				setWhereSqlParamValue(pms, statement);
				if (tbns.size() == 1) {
					return statement.executeUpdate();
				} else {
					pss.add(statement);
				}

			}
			return executeUpdate(pss);
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		} finally {
			getConnectionManager().closeConnection();
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void setParamVal(POJO pojo, Field[] fds, Set<PropInfo> clset, PreparedStatement statement)
			throws SQLException, IllegalAccessException {
		int idx = 1;
		for (PropInfo zd : clset) {
			for (Field fd : fds) {
				Column clm = fd.getAnnotation(Column.class);
				if ((clm != null && clm.name().equals(zd.getCname())) || zd.getCname().equals(fd.getName())) {
					fd.setAccessible(true);
					if (fd.getType().isEnum()) {
						Class<Enum> cls = (Class<Enum>) fd.getType();
						if (fd.isAnnotationPresent(Enumerated.class)
								&& fd.getAnnotation(Enumerated.class).value() == EnumType.STRING) {
							statement.setObject(idx++, fd.get(pojo).toString());
						} else {
							statement.setObject(idx++, Enum.valueOf(cls, fd.get(pojo).toString()).ordinal());
						}
					} else {
						statement.setObject(idx++, fd.get(pojo));
					}
				}

			}

		}
	}

	@Override
	public List<Object> getVlList(String property, Set<Param> params) {
		return getRztPos(property, params, true);
	}

	@Override
	public List<Object> getVlListFromMaster(String property, Set<Param> params) {
		return getRztPos(property, params, false);
	}

	// 单个字段值列表
	private List<Object> getRztPos(String property, Set<Param> params, boolean isRead) {

		if (getCurrentTables().size() < 1) {
			return new ArrayList<>(0);
		}
		try {
			String selectpre = getPreSelectSql(property);
			String whereSqlByParam = getWhereSqlByParam(params);
			Set<String> tbns = getTableNamesByParams(params);
			if (tbns.size() == 1) {
				return getSingleObject(isRead, selectpre + tbns.iterator().next() + whereSqlByParam, params);
			} else {
				List<QueryVo<PreparedStatement>> pss = getqvs(isRead, params, selectpre, whereSqlByParam, tbns);
				return querylist(pss);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		} finally {
			getConnectionManager().closeConnection();
		}

	}

	/// 实体对象列表
	private List<POJO> getRztPos(boolean isRead, Set<Param> params, String... strings) {
		if (getCurrentTables().size() < 1) {
			return new ArrayList<>(0);
		}
		try {
			String selectpre = getPreSelectSql(strings);
			String whereSqlByParam = getWhereSqlByParam(params);
			Set<String> tbns = getTableNamesByParams(params);
			if (tbns.size() == 1) {
				return getSingleObject(isRead, params, selectpre + tbns.iterator().next() + whereSqlByParam, strings);
			} else {
				List<QueryVo<PreparedStatement>> pss = getqvs(isRead, params, selectpre, whereSqlByParam, tbns);
				return querylist(pss, strings);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		} finally {
			getConnectionManager().closeConnection();
		}
	}

	private List<QueryVo<PreparedStatement>> getqvs(boolean isRead, Set<Param> params, String selectpre,
			String whereSqlByParam, Set<String> tbns) throws SQLException {
		List<QueryVo<PreparedStatement>> pss = new ArrayList<>();
		for (String tn : tbns) {
			String sql = selectpre + tn + whereSqlByParam;
			PreparedStatement statement = getStatementBySql(isRead, sql);
			if (getConnectionManager().isShowSql()) {
				log.info(sql);
			}
			setWhereSqlParamValue(params, statement);
			pss.add(new QueryVo<PreparedStatement>(tn, statement));
		}
		return pss;
	}

	@Override
	public PageData<POJO> getPageInfo(int curPage, int pageSize, LinkedHashSet<ObData> orderbys, Set<Param> params,
			String... strings) {
		return new PageData<>(curPage, pageSize, getCount(params),
				getRztPos(true, curPage, pageSize, orderbys, params, strings));
	}

	@Override
	public PageData<POJO> getPageInfoFromMaster(int curPage, int pageSize, LinkedHashSet<ObData> orderbys,
			Set<Param> params, String... strings) {
		return new PageData<>(curPage, pageSize, getCount(params),
				getRztPos(false, curPage, pageSize, orderbys, params, strings));
	}

	/***
	 * 多表分页
	 * 
	 * @param sql
	 * @param curPage
	 * @param pageSize
	 * @return
	 */
	private String getSelectPagingSql(String sql, int curPage, int pageSize) {
		try {
			String dpname = getConnectionManager().getConnection(true).getMetaData().getDatabaseProductName();
			if (dpname.equalsIgnoreCase("MySQL")) {
				return sql + getPagingSql(curPage, pageSize);
			} else if (dpname.equalsIgnoreCase("Oracle")) {
				StringBuilder sb = new StringBuilder("select  row_.*,   rownum  rownum_      from (");
				sb.append(sql);
				sb.append(")  row_  where    rownum <=");
				sb.append(curPage * pageSize);
				return sb.toString();
			}
			throw new IllegalStateException(String.format("当前查询分页路由不支持%s数据库系统", dpname));
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalStateException("无法获取数据库名称");
		}

	}

	/**
	 * 单表分页
	 * 
	 * @param sql
	 * @param curPage
	 * @param pageSize
	 * @return
	 */
	private String getSingleTableSelectPagingSql(String sql, int curPage, int pageSize) {
		try {
			String dpname = getConnectionManager().getConnection(true).getMetaData().getDatabaseProductName();
			if (dpname.equalsIgnoreCase("MySQL")) {
				return sql + getSingleTablePagingSql(curPage, pageSize);
			} else if (dpname.equalsIgnoreCase("Oracle")) {
				return oraclepageselect(sql, curPage, pageSize);
			}
			throw new IllegalStateException(String.format("当前查询分页路由不支持：%s数据库系统", dpname));
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalStateException("无法获取数据库名称");
		}

	}

	private String getSingleTableSelectPagingSqlByStartIndex(int start, String sql, int pageSize) {

		try {
			String dpname = getConnectionManager().getConnection(true).getMetaData().getDatabaseProductName();
			if (dpname.equalsIgnoreCase("MySQL")) {
				return sql + getSinglePagingSql(start, pageSize);
			} else if (dpname.equalsIgnoreCase("Oracle")) {
				return getoracleSinglepagingSelectsql(start, sql, pageSize);
			}
			throw new IllegalStateException(String.format("当前查询分页路由不支持：%s数据库系统", dpname));
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalStateException("无法获取数据库名称");
		}

	}

	/**
	 * Oracle
	 * 
	 * @param sql
	 * @param curPage
	 *            当前页
	 * @param pageSize
	 *            每页显示多少条记录
	 * @return
	 */
	private String oraclepageselect(String sql, int curPage, int pageSize) {
		StringBuilder sb = new StringBuilder(
				"select   *      from        ( select  row_.*,   rownum  rownum_      from (");
		sb.append(sql);
		sb.append(")  row_  where    rownum <=");
		sb.append(curPage * pageSize);
		sb.append(" )   where  rownum_ > ").append((curPage - 1) * pageSize);
		return sb.toString();
	}

	/**
	 * Oracle
	 * 
	 * @param start
	 *            开始位置
	 * @param sql
	 * @param pageSize
	 *            获取多少条记录
	 * @return
	 */
	private String getoracleSinglepagingSelectsql(int start, String sql, int pageSize) {
		if (start < 0 || pageSize < 1) {
			throw new IllegalArgumentException("当开始位置不能小于0或者页大小不能小于1");
		}
		StringBuilder sb = new StringBuilder(
				"select   *      from        ( select  row_.*,   rownum  rownum_      from (");
		sb.append(sql);
		sb.append(")  row_  where    rownum <=");
		sb.append(start + pageSize);
		sb.append(" )   where  rownum_ > ").append(start);
		return sb.toString();

	}

	private List<POJO> getRztPos(Boolean isRead, int curPage, int pageSize, LinkedHashSet<ObData> orderbys,
			Set<Param> params, String... strings) {
		if (curPage < 1 || pageSize < 1 || getCurrentTables().size() < 1) {
			return new ArrayList<>(0);
		}
		Set<String> tbns = getTableNamesByParams(params);
		if (tbns.size() > 1 && (orderbys == null || orderbys.isEmpty())) {
			return getListFromNotSorted(isRead, curPage, pageSize, params, strings).getDataList();
		} else {
			try {
				String selectpre = getPreSelectSql(strings);
				String whereSqlByParam = getWhereSqlByParam(params);
				if (tbns.size() == 1) {
					String sql = getSingleTableSelectPagingSql(
							selectpre + tbns.iterator().next() + whereSqlByParam + getOrderBySql(orderbys), curPage,
							pageSize);
					return getSingleObject(isRead, params, sql, strings);
				} else {
					List<QueryVo<PreparedStatement>> pss = new ArrayList<>();
					for (String tn : tbns) {
						String sql = getSelectPagingSql(selectpre + tn + whereSqlByParam + getOrderBySql(orderbys),
								curPage, pageSize);
						PreparedStatement statement = getStatementBySql(isRead, sql);
						if (getConnectionManager().isShowSql()) {
							log.info(sql);
						}
						setWhereSqlParamValue(params, statement);
						pss.add(new QueryVo<PreparedStatement>(tn, statement));
					}

					List<POJO> querylist = querylist(pss, strings);
					if (querylist.size() > 1) {
						return getOrderbyPagelist(curPage, pageSize, querylist, addsortinfo(orderbys, strings));
					} else {
						return querylist;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new IllegalStateException(e);
			} finally {
				getConnectionManager().closeConnection();
			}
		}

	}

	private List<Object> getSingleObject(Boolean isRead, String sql, Set<Param> params) throws SQLException {
		PreparedStatement statement = getStatementBySql(isRead, sql);
		if (getConnectionManager().isShowSql()) {
			log.info(sql);
		}
		setWhereSqlParamValue(params, statement);
		return getRztObject(statement.executeQuery());
	}

	private List<POJO> getSingleObject(Boolean isRead, Set<Param> params, String sql, String... strings)
			throws SQLException {
		PreparedStatement statement = getStatementBySql(isRead, sql);
		if (getConnectionManager().isShowSql()) {
			log.info(sql);
		}
		setWhereSqlParamValue(params, statement);
		return getRztObject(statement.executeQuery(), strings);
	}

	/**
	 * 不排序分页列表
	 * 
	 * @param isRead
	 * @param curPage
	 * @param pageSize
	 * @param params
	 * @param selectpre
	 * @param whereSqlByParam
	 * @param strings
	 * @return
	 * @throws SQLException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private PageData<POJO> getListFromNotSorted(Boolean isRead, int curPage, int pageSize, Set<Param> params,
			String... strings) {
		if (getConnectionManager().isShowSql()) {
			log.info("begin........................................");
		}
		try {
			String selectpre = getPreSelectSql(strings);
			String whereSqlByParam = getWhereSqlByParam(params);
			List<QueryVo<PreparedStatement>> pss = new ArrayList<>();
			List<QueryVo<Long>> qvs = getCountPerTable(isRead, params);
			// 开始位置
			int start = getPageStartIndex(curPage, pageSize);
			// 当前所有查到的最大位置
			int csum = 0;
			// 当前已经可以查到的数据量
			int rdsum = 0;
			for (QueryVo<Long> q : qvs) {
				csum += q.getOv();
				if (rdsum < pageSize) {
					if (csum > start) {
						// 当前 表开始位置
						int startindex = 0;
						// 还剩多少数据需要查询
						int left = pageSize - rdsum;
						int initSize = q.getOv().intValue() > left ? left : q.getOv().intValue();
						if (start > 0) {
							// 当前表查询剩余多少记录
							int step = csum - start;
							if (step < q.getOv().intValue()) {
								startindex = q.getOv().intValue() - step;
								if (step < pageSize) {
									initSize = step;
								}
							}
						}
						rdsum += initSize;
						String sql = getSingleTableSelectPagingSqlByStartIndex(startindex,
								selectpre + q.getTbn() + whereSqlByParam, initSize);
						PreparedStatement statement = getStatementBySql(isRead, sql);
						if (getConnectionManager().isShowSql()) {
							log.info(sql);
						}
						setWhereSqlParamValue(params, statement);
						pss.add(new QueryVo<PreparedStatement>(q.getTbn(), statement));
					}
				} else {
					break;
				}
			}
			return new PageData<>(curPage, pageSize, qvs.stream().mapToLong(QueryVo::getOv).sum(),
					querylist(pss, strings));
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		} finally {
			getConnectionManager().closeConnection();
			if (getConnectionManager().isShowSql()) {
				log.info("........................................end");
			}
		}

	}

	/**
	 * 开始位置
	 * 
	 * @param curPage
	 * @param pageSize
	 * @return
	 */
	private int getPageStartIndex(int curPage, int pageSize) {
		int start = (curPage - 1) * pageSize;
		return start;
	}

	private LinkedHashSet<SortInfo> addsortinfo(LinkedHashSet<ObData> orderbys, String... strings) {
		LinkedHashSet<SortInfo> sts = new LinkedHashSet<>();
		if (orderbys != null && orderbys.size() > 0) {
			List<String> asList = Arrays.asList(strings);
			for (ObData ob : orderbys) {
				if ((strings != null && strings.length > 0) && asList.contains(ob.getPropertyName())) {
					sts.add(new SortInfo(ob.getPropertyName(), ob.getIsDesc()));
				} else {
					sts.add(new SortInfo(ob.getPropertyName(), ob.getIsDesc()));
				}
			}

		}
		return sts;
	}

	/// 获取每个表的记录数
	private List<QueryVo<Long>> getCountPerTable(Boolean isRead, Set<Param> params) {
		List<QueryVo<Long>> qvs = new ArrayList<>();
		try {
			List<Future<QueryVo<ResultSet>>> rzts = invokeall(isRead, params, KSentences.SELECT_COUNT.getValue());
			for (Future<QueryVo<ResultSet>> f : rzts) {
				ResultSet rs = f.get().getOv();
				if (rs.next()) {
					long cc = rs.getLong(1);
					if (cc > 0) {
						qvs.add(new QueryVo<Long>(f.get().getTbn(), cc));
					}
				}
			}
			if (qvs.size() > 1) {
				qvs.sort(new Comparator<QueryVo<Long>>() {
					@Override
					public int compare(QueryVo<Long> o1, QueryVo<Long> o2) {
						return o2.getTbn().compareTo(o1.getTbn());
					}
				});
			}
		} catch (Exception e) {
			throw new IllegalStateException(e);
		} finally {
			getConnectionManager().closeConnection();
		}
		return qvs;
	}

	private <T> List<T> getOrderbyPagelist(int curPage, int pageSize, List<T> querylist, LinkedHashSet<SortInfo> sts) {
		if (sts != null && sts.size() > 0) {
			querylist.sort(new SortComparator<>(sts));
		}
		int fromIndex = getPageStartIndex(curPage, pageSize);
		int toIndex = fromIndex + pageSize;
		if (toIndex > querylist.size()) {
			toIndex = querylist.size();
		}
		if (fromIndex >= toIndex) {
			return new ArrayList<>(0);
		}
		return querylist.subList(fromIndex, toIndex);
	}

	private String getOrderBySql(LinkedHashSet<ObData> orderbys) {
		StringBuilder sb = new StringBuilder();
		if (orderbys != null && orderbys.size() > 0) {
			sb.append(KSentences.ORDERBY.getValue());
			Iterator<ObData> ite = orderbys.iterator();
			while (ite.hasNext()) {
				ObData ob = ite.next();
				for (PropInfo p : getPropInfos()) {
					if (p.getPname().equals(ob.getPropertyName().trim())) {
						if (ob.getFunName() != null && ob.getFunName().trim().length() > 0) {
							sb.append(ob.getFunName());
							sb.append("(");
							sb.append(p.getCname());
							sb.append(")");
						} else {
							sb.append(p.getCname());
						}
						if (ob.getIsDesc()) {
							sb.append(KSentences.DESC.getValue());
						}
						if (ite.hasNext()) {
							sb.append(KSentences.COMMA.getValue());
						}
					}
				}
			}
		}

		return sb.toString();
	}

	/**
	 * 多表分页
	 * 
	 * @param curPage
	 * @param pageSize
	 * @return
	 */
	private String getPagingSql(int curPage, int pageSize) {
		if (curPage < 1 || pageSize < 1) {
			throw new IllegalArgumentException("当前页和页大小不能小于0");
		}
		StringBuilder sb = new StringBuilder(KSentences.LIMIT.getValue());
		sb.append(curPage * pageSize);
		return sb.toString();
	}

	/**
	 * 单表分页
	 * 
	 * @param curPage
	 * @param pageSize
	 * @return
	 */
	private String getSingleTablePagingSql(int curPage, int pageSize) {
		if (curPage < 1 || pageSize < 1) {
			throw new IllegalArgumentException("当前页和页大小不能小于0");
		}
		StringBuilder sb = new StringBuilder(KSentences.LIMIT.getValue());
		sb.append((curPage - 1) * pageSize);
		sb.append(KSentences.COMMA.getValue()).append(pageSize);
		return sb.toString();
	}

	/**
	 * 不排序分页查询
	 * 
	 * @param start
	 *            开始位置
	 * @param pageSize
	 *            获取多少条数据
	 * @return
	 */
	private String getSinglePagingSql(int start, int pageSize) {
		if (start < 0 || pageSize < 1) {
			throw new IllegalArgumentException("当开始位置不能小于0或者页大小不能小于1");
		}
		StringBuilder sb = new StringBuilder(KSentences.LIMIT.getValue());
		sb.append(start);
		sb.append(KSentences.COMMA.getValue()).append(pageSize);
		return sb.toString();
	}

	private List<Object> querylist(List<QueryVo<PreparedStatement>> pss)
			throws InterruptedException, ExecutionException {
		if (pss != null && pss.size() > 0) {
			List<Future<QueryVo<ResultSet>>> rzs = invokeQueryAll(pss);
			List<Object> pos = new ArrayList<>();
			for (Future<QueryVo<ResultSet>> f : rzs) {
				pos.addAll(getRztObject(f.get().getOv()));
			}
			return pos;
		} else {
			return new ArrayList<>(0);
		}
	}

	private List<POJO> querylist(List<QueryVo<PreparedStatement>> pss, String... strings)
			throws InterruptedException, ExecutionException {
		if (pss != null && pss.size() > 0) {
			List<Future<QueryVo<ResultSet>>> rzs = invokeQueryAll(pss);
			List<POJO> pos = new ArrayList<>();
			for (Future<QueryVo<ResultSet>> f : rzs) {

				try {
					pos.addAll(getRztObject(f.get().getOv(), strings));
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}
			return pos;

		}
		return new ArrayList<>(0);
	}

	private List<Object[]> getObjectList(ResultSet resultSet) throws SQLException {
		List<Object[]> objs = new ArrayList<>();
		while (resultSet.next()) {
			int columnCount = resultSet.getMetaData().getColumnCount();
			Object[] os = new Object[columnCount];
			for (int i = 1; i <= columnCount; i++) {
				os[i - 1] = resultSet.getObject(i);
			}
			objs.add(os);
		}
		return objs;
	}

	private List<Future<QueryVo<ResultSet>>> invokeQueryAll(List<QueryVo<PreparedStatement>> pss) {
		List<QueryCallable> qcs = new ArrayList<>();
		for (QueryVo<PreparedStatement> ps : pss) {
			qcs.add(new QueryCallable(ps.getOv(), ps.getTbn()));
			if (getConnectionManager().isShowSql()) {
				log.error(ps.getOv().toString());
			}
		}
		try {
			return NEW_FIXED_THREAD_POOL.invokeAll(qcs);
		} catch (Throwable e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
	}

	private int executeUpdate(List<PreparedStatement> pss) throws InterruptedException, ExecutionException {
		int ttc = 0;
		List<Future<Integer>> rzts = invokeUpdateAll(pss);
		for (Future<Integer> f : rzts) {
			ttc += f.get();
		}
		return ttc;
	}

	private List<Future<Integer>> invokeUpdateAll(List<PreparedStatement> pss) {

		List<UpdateCallable> ucs = new ArrayList<>();
		for (PreparedStatement ps : pss) {
			ucs.add(new UpdateCallable(ps));
		}
		try {
			return NEW_FIXED_THREAD_POOL.invokeAll(ucs);
		} catch (Throwable e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}

	}

	private List<Object> getRztObject(ResultSet rs) {
		try {
			List<Object> ts = new ArrayList<>();
			while (rs.next()) {
				ts.add(rs.getObject(1));
			}
			return ts;
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
	}

	protected List<POJO> getRztObject(ResultSet rs, String... strings) {
		List<POJO> pos = new ArrayList<>();
		try {
			Set<PropInfo> pis = getPropInfos();
			while (rs.next()) {
				POJO po = clazz.newInstance();
				if (strings != null && strings.length > 0) {
					a: for (int i = 0; i < strings.length; i++) {
						for (PropInfo pi : pis) {
							if (pi.getPname().equals(strings[i])) {
								Field fd = clazz.getDeclaredField(strings[i]);
								MyObjectUtils.setObjectValue(fd, rs.getObject(i + 1), po);
								continue a;
							}
						}
					}
				} else {
					a: for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
						for (PropInfo pi : pis) {
							if (pi.getCname().equals(rs.getMetaData().getColumnName(i + 1))) {
								Field fd = clazz.getDeclaredField(pi.getPname());
								MyObjectUtils.setObjectValue(fd, rs.getObject(i + 1), po);
								continue a;
							}
						}
					}
				}
				pos.add(po);
			}
			return pos;
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}

	}

	/**
	 * 设置查询条件
	 * 
	 * @param sb
	 * @param pms
	 */
	protected String getWhereSqlByParam(Set<Param> pms) {
		StringBuilder sb = new StringBuilder();
		if (pms != null && pms.size() > 0) {
			sb.append(KSentences.WHERE.getValue());
			geneConditionSql(pms, sb);
		}
		return sb.toString();
	}

	private void geneConditionSql(Set<Param> pms, StringBuilder sb) {
		Iterator<Param> pmsite = pms.iterator();
		while (pmsite.hasNext()) {
			Param pm = pmsite.next();
			boolean isor = pm.getOrParam() != null;
			if (isor) {
				sb.append("(");
			}
			do {
				for (PropInfo p : getPropInfos()) {
					if (p.getPname().equals(pm.getPname())) {
						if (pm.getCdType().equals(PmType.OG)) {
							setogcds(sb, pm, p);

						} else {

							setvlcds(sb, pm, p);
						}
					}
				}
				pm = pm.getOrParam();
				if (pm != null) {
					sb.append(KSentences.OR.getValue());
				}
			} while (pm != null);
			if (isor) {
				sb.append(")");
			}
			if (pmsite.hasNext()) {
				sb.append(KSentences.AND.getValue());
			}
		}
	}

	private void setogcds(StringBuilder sb, Param pm, PropInfo p) {
		setcName(sb, pm, p);
		if (pm.getOperators().equals(Operate.BETWEEN)) {
			sb.append(pm.getOperators().getValue());
			sb.append(pm.getFirstValue());
			sb.append(KSentences.AND);
			sb.append(pm.getValue());
		} else if (pm.getOperators().equals(Operate.IN)
				|| pm.getOperators().equals(Operate.NOT_IN) && pm.getInValue() != null) {
			sb.append(pm.getOperators().getValue());
			sb.append("(");
			sb.append(pm.getValue());
			sb.append(")");
		} else {
			if (pm.getValue() != null && !pm.getValue().toString().trim().equals("")) {
				sb.append(pm.getOperators().getValue()).append(pm.getValue());
			} else {
				throw new IllegalArgumentException("非法的条件查询：CdType.OG类型的条件值不能为空");
			}
		}
	}

	private void setvlcds(StringBuilder sb, Param pm, PropInfo p) {
		if (pm.getOperators().equals(Operate.BETWEEN)) {
			setcName(sb, pm, p);

			sb.append(pm.getOperators().getValue());
			sb.append(KSentences.POSITION_PLACEHOLDER);
			sb.append(KSentences.AND);
			sb.append(KSentences.POSITION_PLACEHOLDER);

		} else if (pm.getOperators().equals(Operate.IN)
				|| pm.getOperators().equals(Operate.NOT_IN) && pm.getInValue() != null) {
			setcName(sb, pm, p);
			sb.append(pm.getOperators().getValue());
			sb.append("(");
			for (int i = 0; i < pm.getInValue().size(); i++) {
				sb.append(KSentences.POSITION_PLACEHOLDER);
				if (i < pm.getInValue().size() - 1) {
					sb.append(KSentences.COMMA.getValue());
				}
			}
			sb.append(")");

		} else {
			if (pm.getValue() != null && !pm.getValue().toString().trim().equals("")) {
				setcName(sb, pm, p);
				sb.append(pm.getOperators().getValue()).append(KSentences.POSITION_PLACEHOLDER.getValue());
			} else if (pm.getOperators().equals(Operate.EQ) || pm.getOperators().equals(Operate.NOT_EQ)) {
				if (getPmsType(pm) == String.class) {
					sb.append("(");
					setcName(sb, pm, p);
					sb.append(pm.getOperators().getValue()).append("''");
					if (pm.getOperators().equals(Operate.EQ)) {
						sb.append(KSentences.OR.getValue());
					} else {
						sb.append(KSentences.AND.getValue());
					}
				}
				if (pm.getOperators().equals(Operate.EQ)) {
					setcName(sb, pm, p);
					sb.append(KSentences.IS_NULL.getValue());
				} else {
					setcName(sb, pm, p);
					sb.append(KSentences.IS_NOT_NULL.getValue());
				}
				if (getPmsType(pm) == String.class) {
					sb.append(")");
				}
			}
		}
	}

	private Class<?> getPmsType(Param pm) {
		for (PropInfo p : getPropInfos()) {
			if (p.getPname().equals(pm.getPname())) {
				return p.getType();
			}
		}
		throw new IllegalArgumentException(String.format("%s字段没有定义...", pm.getPname()));
	}

	private void setcName(StringBuilder sb, Param pm, PropInfo p) {
		if (pm.getCdType().equals(PmType.FUN)) {
			sb.append(pm.getFunName()).append("(");
		}
		sb.append(p.getCname());
		if (pm.getCdType().equals(PmType.FUN)) {
			sb.append(")");

		}
	}

	protected int setWhereSqlParamValue(Set<Param> pms, PreparedStatement statement, int ix) {

		if (pms != null && pms.size() > 0) {

			for (Param pm : pms) {

				do {
					try {

						if (!pm.getCdType().equals(PmType.OG)) {
							if (pm.getOperators().equals(Operate.BETWEEN)) {
								statement.setObject(ix++, getParamSqlValue(pm.getFirstValue(), pm.getPname()));
								statement.setObject(ix++, getParamSqlValue(pm.getValue(), pm.getPname()));
							} else if (pm.getOperators().equals(Operate.IN)
									|| pm.getOperators().equals(Operate.NOT_IN) && pm.getInValue() != null) {
								for (Object se : pm.getInValue()) {
									statement.setObject(ix++, getParamSqlValue(se, pm.getPname()));
								}
							} else {
								if (pm.getValue() != null && !pm.getValue().toString().trim().equals("")) {
									statement.setObject(ix++, getParamSqlValue(pm.getValue(), pm.getPname()));
								}
							}
						}
					} catch (SQLException e) {
						e.printStackTrace();
						throw new IllegalArgumentException(e);
					}
					pm = pm.getOrParam();
				} while (pm != null);

			}

		}
		return ix;

	}

	private Object getParamSqlValue(Object o, String pname) {
		if (o != null && o.getClass().isEnum()) {
			EnumType et = isEnum(pname);
			if (et != null) {
				if (et.equals(EnumType.STRING)) {
					return o.toString();
				} else {
					PropInfo pp = getPropInfo(pname);
					Class<Enum> cls = (Class<Enum>) pp.getType();
					return Enum.valueOf(cls, o.toString()).ordinal();
				}
			}
		}
		return o;
	}

	protected PropInfo getPropInfo(String pname) {
		if (pname != null && pname.trim().length() > 0) {
			Set<PropInfo> pps = getPropInfos();
			for (PropInfo pp : pps) {
				if (pp.getPname().equals(pname)) {
					return pp;
				}
			}
		}
		return null;
	}

	protected EnumType isEnum(String pname) {
		if (pname != null) {
			Set<PropInfo> pps = getPropInfos();
			for (PropInfo pp : pps) {
				if (pp.getPname().equals(pname) && pp.getType().isEnum()) {
					return pp.getEnumType();
				}
			}
		}
		return null;
	}

	/**
	 * 给查询条件赋值
	 * 
	 * @param pms
	 * @param statement
	 */
	protected void setWhereSqlParamValue(Set<Param> pms, PreparedStatement statement) {
		setWhereSqlParamValue(pms, statement, 1);

	}

	/**
	 * 根据条件得到数据所在的表
	 * 
	 * @param pms
	 * @return
	 */
	protected Set<String> getTableNamesByParams(Set<Param> pms) {
		if (pms != null && pms.size() > 0) {
			Entry<String, LinkedHashSet<PropInfo>> tbimp = ConnectionManager.getTbinfo(clazz).entrySet().iterator()
					.next();
			for (Param pm : pms) {
				for (PropInfo p : tbimp.getValue()) {
					if (p.getColumnRule() != null) {
						if (pm.getPname().equals(p.getPname()) && pm.getOrParam() == null) {

							if (pm.getOperators().equals(Operate.EQ) && pm.getValue() != null) {
								String tableName = gettbName(tbimp, pm, p);
								if (isContainsTable(tableName)) {
									return new HashSet<>(Arrays.asList(tableName));
								}
							} else if (pm.getOperators().equals(Operate.IN) && pm.getInValue() != null
									&& pm.getInValue().size() > 0) {
								Set<String> tbns = new HashSet<>();
								for (Object sid : pm.getInValue()) {
									if (sid != null) {
										String tableName = getTableName(
												getTableMaxIdx(sid, p.getType(), p.getColumnRule()), tbimp.getKey());
										if (isContainsTable(tableName)) {
											tbns.add(tableName);
										}
									}
								}
								if (tbns.size() > 0) {
									return tbns;
								}
							} else if (p.getColumnRule().ruleType().equals(RuleType.RANGE)
									&& pm.getOperators().equals(Operate.BETWEEN) && pm.getValue() != null
									&& pm.getFirstValue() != null) {
								long st = getTableMaxIdx(pm.getFirstValue(), p.getType(), p.getColumnRule());
								long ed = getTableMaxIdx(pm.getValue(), p.getType(), p.getColumnRule());
								Set<String> nms = gettbs(tbimp, st, ed);
								if (nms.size() > 0) {
									return nms;
								}
							} else if (p.getColumnRule().ruleType().equals(RuleType.RANGE)
									&& pm.getOperators().equals(Operate.GE) && pm.getValue() != null) {

								long st = getTableMaxIdx(pm.getValue(), p.getType(), p.getColumnRule());
								if (st > 0) {
									int len = getTableName(st, tbimp.getKey())
											.split(KSentences.SHARDING_SPLT.getValue()).length;

									long ed = getCurrentTables().stream().mapToLong(n -> {
										String[] arr = n.split(KSentences.SHARDING_SPLT.getValue());
										if (arr.length == len) {
											return Long.valueOf(arr[arr.length - 1]);
										}
										return 0L;
									}).max().getAsLong();

									Set<String> nms = gettbs(tbimp, st, ed);
									if (nms.size() > 0) {
										return nms;
									}
								}
							}
						}
					}
				}
			}
		}

		return getCurrentTables();
	}

	private Set<String> gettbs(Entry<String, LinkedHashSet<PropInfo>> tbimp, long st, long ed) {
		Set<String> nms = new HashSet<>();
		for (long i = st; i <= ed; i++) {
			String tableName = getTableName(i, tbimp.getKey());
			if (isContainsTable(tableName)) {
				nms.add(tableName);
			}
		}
		return nms;
	}

	private String gettbName(Entry<String, LinkedHashSet<PropInfo>> tbimp, Param pm, PropInfo p) {
		return getTableName(getTableMaxIdx(pm.getValue(), p.getType(), p.getColumnRule()), tbimp.getKey());

	}

	private boolean isContainsTable(String tbname) {
		Iterator<String> ite = getCurrentTables().iterator();
		while (ite.hasNext()) {
			String tn = ite.next();
			if (tn.trim().equalsIgnoreCase(tbname.trim())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 当前操作的实体对象类型
	 */
	private Class<POJO> clazz = getThisClass(getClass());

	/**
	 * 获取数据库连接
	 * 
	 * @return
	 */
	protected abstract IConnectionManager getConnectionManager();

	@SuppressWarnings("unchecked")
	private Class<POJO> getThisClass(Class<?> clazz) {
		Type type = clazz.getGenericSuperclass();
		if (type instanceof ParameterizedType) {
			Type[] ts = ((ParameterizedType) type).getActualTypeArguments();
			return (Class<POJO>) ts[0];
		}
		throw new IllegalStateException("DAO 继承出现错误！DAO的父类需要使用泛型却没有使用泛型。。");
	}

	private static String getTableName(Long max, String name) {
		if (max < 1) {
			return name;
		}
		return name + KSentences.SHARDING_SPLT.getValue() + max;

	}

	/**
	 * 得到字段信息
	 * 
	 * @return
	 */
	protected Set<PropInfo> getPropInfos() {
		return ConnectionManager.getTbinfo(clazz).entrySet().iterator().next().getValue();
	}

	private StringBuilder getSelectSql(String tableName, String... strings) {
		StringBuilder sb = new StringBuilder(getPreSelectSql(strings));
		sb.append(tableName);

		return sb;
	}

	private String getPreSelectSql(String... strings) {
		StringBuilder sb = new StringBuilder(KSentences.SELECT.getValue());
		if (strings != null && strings.length > 0) {
			for (int i = 0; i < strings.length; i++) {
				for (PropInfo pi : getPropInfos()) {
					if (strings[i].equals(pi.getPname())) {
						sb.append(pi.getCname());
						break;
					}
				}
				if (i < strings.length - 1) {
					sb.append(KSentences.COMMA.getValue());
				}
			}
		} else {
			sb.append(KSentences.SELECT_ALL);
		}
		sb.append(KSentences.FROM.getValue());
		return sb.toString();
	}

	private String[] getGSelect(String[] gbs, Collection<String> vvs) {
		LinkedHashSet<String> rz = new LinkedHashSet<>();
		for (String g : gbs) {
			rz.add(g.trim());
		}
		if (vvs != null) {
			for (String v : vvs) {
				rz.add(v.trim());
			}
		}
		return rz.toArray(new String[0]);
	}

	/**
	 * 判断表是否已经被创建
	 * 
	 * @param tblname
	 * @return
	 */
	private boolean isExistTable(String tblname) {
		Set<String> tbns = getCurrentTables();
		for (String tn : tbns) {
			if (tn.trim().equalsIgnoreCase(tblname.trim())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 单表表拆分最大数量
	 */
	private volatile int maxTableCount = 1024;
	/**
	 * 实体类对应的当前已经分表的表名集合
	 */
	private volatile static ConcurrentHashMap<Class<?>, ConcurrentSkipListSet<String>> CUR_TABLES = new ConcurrentHashMap<Class<?>, ConcurrentSkipListSet<String>>();

	protected Set<String> getCurrentTables() {
		ConcurrentSkipListSet<String> tbns = CUR_TABLES.get(clazz);
		if (tbns == null) {
			synchronized (CUR_TABLES) {
				if (tbns == null) {
					tbns = reFreshTables();
				}
			}
		}
		return tbns;
	}

	@Override
	public void refreshCurrentTables() {
		reFreshTables();
	}

	private ConcurrentSkipListSet<String> reFreshTables() {
		try {
			ResultSet rs = getTableMeta(getConnectionManager().getConnection());
			ConcurrentSkipListSet<String> tbns = new ConcurrentSkipListSet<String>();
			String srctb = ConnectionManager.getTbinfo(clazz).entrySet().iterator().next().getKey();
			while (rs.next()) {
				String dbtbn = rs.getString("TABLE_NAME");
				String schem = rs.getString("TABLE_SCHEM");
				String[] tbsps = dbtbn.toUpperCase().split("^" + srctb.toUpperCase());
				char z = 'n';
				if (tbsps.length == 2) {
					String ts = tbsps[1].replaceAll("_", "");
					if (ts.length() == 0) {
						z = 0;
					} else {
						z = ts.charAt(0);
					}
				}
				if (tbsps.length == 0 || (z >= '0' && z <= '9') || (z >= 0 && z <= 9)) {
					if (schem != null && schem.length() > 0) {
						dbtbn = schem + "." + dbtbn;
					}
					if (dbtbn.startsWith(srctb)) {
						tbns.add(dbtbn);
					}
				}
			}
			CUR_TABLES.put(clazz, tbns);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		} finally {
			getConnectionManager().closeConnection();
		}
		return CUR_TABLES.get(clazz);
	}

	/**
	 * 根据实体自动创建表，默认支持MYSQL，如果需要支持其他数据库，请在子类重写这个方法
	 * 
	 * @param ctbname
	 *            需要创建的表名
	 */
	protected String createTable(String ctbname) {
		Set<PropInfo> pis = getPropInfos();
		if (pis.size() > 0) {
			StringBuilder ctbsb = new StringBuilder(KSentences.CREATE_TABLE.getValue());
			ctbsb.append(ctbname).append("(");
			Iterator<PropInfo> pisite = pis.iterator();
			while (pisite.hasNext()) {
				PropInfo p = pisite.next();
				getdbtype(ctbsb, p);
				if (pisite.hasNext()) {
					ctbsb.append(KSentences.COMMA.getValue());
				}
			}
			ctbsb.append(")");
			return ctbsb.toString();
		}
		return "";
	}

	protected void getdbtype(StringBuilder ctbsb, PropInfo p) {
		ctbsb.append(p.getCname()).append("   ");
		if (p.getType() == Integer.class) {
			ctbsb.append("INT");
		} else if (p.getType() == Float.class) {
			ctbsb.append("FLOAT");
		} else if (p.getType() == Long.class) {
			ctbsb.append("BIGINT");
		} else if (p.getType() == Double.class) {
			ctbsb.append("Double");
		} else if (p.getType() == Boolean.class) {
			ctbsb.append("BIT");
		} else if (p.getType() == Date.class) {
			try {
				Field fd = clazz.getDeclaredField(p.getPname());
				Temporal tp = fd.getAnnotation(Temporal.class);
				if (tp != null && tp.value().equals(TemporalType.TIMESTAMP)) {
					ctbsb.append("DATETIME");
				} else if (tp != null && tp.value().equals(TemporalType.TIME)) {
					ctbsb.append("TIME");
				} else {
					ctbsb.append("DATE");
				}
			} catch (NoSuchFieldException | SecurityException e) {
				e.printStackTrace();
				throw new IllegalStateException(e);
			}
		} else if (p.getType() == Time.class) {
			ctbsb.append("TIME");
		} else if (p.getType() == Timestamp.class) {
			ctbsb.append("DATETIME");
		} else if (p.getType() == String.class) {
			if (p.getIsLob()) {
				ctbsb.append("LONGTEXT");
			} else {
				ctbsb.append("VARCHAR(").append(p.getLength()).append(")");
			}
		} else if (p.getType() == byte[].class) {
			ctbsb.append("LONGBLOB");
		} else if (p.getType().isEnum()) {
			try {
				Field fd = clazz.getDeclaredField(p.getPname());
				Enumerated enm = fd.getAnnotation(Enumerated.class);
				if (enm != null && enm.value() == EnumType.STRING) {
					ctbsb.append("VARCHAR(").append(p.getLength()).append(")");
				} else {
					ctbsb.append("INT");
				}
			} catch (NoSuchFieldException | SecurityException e) {
				e.printStackTrace();
				throw new IllegalStateException(e);
			}
		}

		if (p.getIsPrimarykey()) {
			ctbsb.append("  PRIMARY KEY  ");
			if (p.getAutoIncreament()) {
				ctbsb.append("  AUTO_INCREMENT  ");
			}
		} else {
			if (p.getIsNotNull()) {
				ctbsb.append("  NOT NULL  ");
			}
			if (p.getIsUnique()) {
				ctbsb.append("  UNIQUE  ");
			}
		}
	}

	@PostConstruct
	public void init() {
		if (getConnectionManager().isGenerateDdl()) {
			NEW_FIXED_THREAD_POOL.execute(() -> createFirstTable());
		}
	}

	private void createFirstTable() {
		try {
			String tableName = clazz.getSimpleName();
			if (clazz.isAnnotationPresent(Table.class)) {
				String tbn = clazz.getAnnotation(Table.class).name().trim();
				if (tbn.length() > 0) {
					tableName = tbn;
				}
			}
			boolean isNotExists = true;
			Connection connection = getConnectionManager().getConnection();
			ResultSet rs = getTableMeta(connection);
			while (rs.next()) {
				String rzn = rs.getString("TABLE_NAME");
				if (rzn.equalsIgnoreCase(tableName)) {
					isNotExists = false;
					break;
				}
			}
			Set<PropInfo> pps = getPropInfos();
			if (isNotExists) {
				String csql = createTable(tableName);
				if (csql != null && csql.trim().length() > 0) {
					if (getConnectionManager().isShowSql()) {
						log.info(csql);
					}
					getConnectionManager().getConnection().prepareStatement(csql).executeUpdate();
				}
			} else {

				ResultSet crs = connection.getMetaData().getColumns(connection.getCatalog(), null, tableName, null);
				List<String> cnames = new ArrayList<>();
				while (crs.next()) {
					cnames.add(crs.getString("COLUMN_NAME"));
				}
				List<PropInfo> ncns = new ArrayList<>();
				a: for (PropInfo pi : pps) {
					for (String cn : cnames) {
						if (cn.equalsIgnoreCase(pi.getCname())) {
							continue a;
						}
					}
					ncns.add(pi);
				}

				if (ncns.size() > 0) {
					StringBuilder sb = new StringBuilder();
					Iterator<PropInfo> ite = ncns.iterator();
					while (ite.hasNext()) {
						PropInfo nextcn = ite.next();
						getdbtype(sb, nextcn);
						if (ite.hasNext()) {
							sb.append(KSentences.COMMA.getValue());
						}
					}
					if (sb.length() > 0) {
						String avl = sb.toString();
						for (String t : getCurrentTables()) {
							try {
								String sql = String.format(ALTER_TABLE_S_ADD_S, t, avl);
								if (getConnectionManager().isShowSql()) {
									log.info(sql);
								}
								getConnectionManager().getConnection().prepareStatement(sql).executeUpdate();

							} catch (Throwable e) {
								e.printStackTrace();
							}
						}
					}
				}
			}
			for (PropInfo p : pps) {
				if (isCreateIndex(tableName, p)) {
					for (String t : getCurrentTables()) {
						try {
							// 当前索引的名称
							String sql = String.format(ALTER_TABLE_S_ADD_INDEX_S, t,
									(p.getIndex().unique() ? " UNIQUE " : ""), getCurrentIndexName(p),
									getIndexColumns(p));
							if (getConnectionManager().isShowSql()) {
								log.info(sql);
							}
							getConnectionManager().getConnection().prepareStatement(sql).executeUpdate();

						} catch (Throwable e) {
							e.printStackTrace();
							log.error("创建索引报错", e);
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		} finally {
			getConnectionManager().closeConnection();
		}
	}

	private String getIndexColumns(PropInfo p) {
		StringBuilder sbd = new StringBuilder(p.getCname());
		if (p.getType() == String.class && p.getLength() > p.getIndex().length()) {
			sbd.append("(").append(p.getIndex().length()).append(")");
		}
		if (p.getIndex().secondPropName() != null && !"".equals(p.getIndex().secondPropName().trim())) {
			PropInfo propInfo = getPropInfo(p.getIndex().secondPropName());
			if (propInfo != null) {
				sbd.append(KSentences.COMMA.getValue()).append(propInfo.getCname());
				if (propInfo.getType() == String.class) {
					sbd.append("(").append(p.getIndex().length()).append(")");
				}
			}
		}

		return sbd.toString();
	}

	private boolean isCreateIndex(String tbn, PropInfo p) throws SQLException {
		// 是否创建索引
		if (p.getIndex() != null) {
			ResultSet saa = getConnectionManager().getConnection().getMetaData().getIndexInfo(null, null, tbn,
					p.getIndex().unique(), false);
			// 当前索引的名称
			String idxName = getCurrentIndexName(p);
			/**
			 * 统计目前数据索引数据
			 */
			Map<String, String> grps = new HashMap<>(5);
			while (saa.next()) {
				String idn = saa.getString("INDEX_NAME");
				if (idn.equals(idxName)) {
					return false;
				}
				String cn = saa.getString("COLUMN_NAME");
				if (grps.get(idn) != null) {
					grps.put(idn, grps.get(idn) + cn);
				} else {
					grps.put(idn, cn);
				}
			}
			PropInfo propInfo = getPropInfo(p.getIndex().secondPropName());
			if (!grps.containsKey(idxName)
					&& !grps.containsValue(p.getCname() + (propInfo == null ? "" : propInfo.getCname()))) {
				return true;
			}

		}
		return false;
	}

	private String getCurrentIndexName(PropInfo p) {
		String idxName = p.getIndex().name().equals("") ? p.getCname() + INDEX_SUBFIX : p.getIndex().name();
		return idxName;
	}

	private static ResultSet getTableMeta(Connection conn) throws SQLException {
		DatabaseMetaData metaData = conn.getMetaData();
		return metaData.getTables(conn.getCatalog(), null, null, new String[] { "TABLE" });
	}

	protected void setMaxTableCount(int maxTableCount) {
		this.maxTableCount = maxTableCount;
	}

	private static final String INDEX_SUBFIX = "_idx";
	private static final String ALTER_TABLE_S_ADD_S = " ALTER  table  %s  add  (%s)";
	private static final String ALTER_TABLE_S_ADD_INDEX_S = "ALTER  table  %s  add  %s  index  %s(%s)";
	private static final ForkJoinPool NEW_FIXED_THREAD_POOL = new ForkJoinPool(
			Integer.min(Runtime.getRuntime().availableProcessors() * 30, 150));
}
