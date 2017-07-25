package com.fd.myshardingfordata.dao.base;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fd.myshardingfordata.em.StatisticsType;
import com.fd.myshardingfordata.helper.ObData;
import com.fd.myshardingfordata.helper.PageData;
import com.fd.myshardingfordata.helper.Param;

/***
 * DAO通用操作接口
 * 
 * @author 符冬
 *
 * @param <POJO>
 */
public interface IBaseShardingDao<POJO> {
	/**
	 * 查询总记录数
	 * 
	 * @param pms
	 * @return
	 */
	Long getCount(Set<Param> pms);

	/**
	 * 保存
	 * 
	 * @param pojo
	 * @return
	 */
	Integer save(POJO pojo);

	/**
	 * 更新
	 * 
	 * @return
	 */
	Integer update(Set<Param> pms, Map<String, Object> mps);

	/**
	 * 根据条件删除
	 * 
	 * @return
	 */
	Integer delete(Set<Param> pms);

	/**
	 * 根据主键删除
	 * 
	 * @param id
	 * @return
	 */
	Integer deleteById(Serializable... id);

	/**
	 * 根据条件查询所有记录
	 * 
	 * @param pms
	 * @param cls
	 * @return
	 */
	List<POJO> getList(Set<Param> pms, String... cls);

	/**
	 * 根据条件查询排序列表
	 * 
	 * @param orderby
	 * @param pms
	 * @param cls
	 * @return
	 */
	List<POJO> getListAndOrderBy(LinkedHashSet<ObData> orderbys, Set<Param> pms, String... cls);

	/**
	 * 查询排序列表
	 * 
	 * @param orderby
	 * @param cls
	 * @return
	 */
	List<POJO> getListAndOrderBy(LinkedHashSet<ObData> orderbys, String... cls);

	/**
	 * 根据主键查询对象
	 * 
	 * @param id
	 * @return 如果不存在返回 null
	 */
	POJO getById(Serializable id, String... cls);

	/**
	 * 根据唯一限定条件查询对象
	 * 
	 * @param propertyName
	 * @param value
	 * @param c
	 * @return 如果字段值不唯一有多条返回 null
	 */
	POJO get(String propertyName, Serializable value, String... cls);

	/**
	 * 根据多个限定条件查询对象
	 * 
	 * @param pms
	 * @param c
	 * @return 如果有多条返回 null
	 */
	POJO get(Set<Param> pms, String... cls);

	/**
	 * 根据主键列表查询
	 * 
	 * @param ids
	 * @param strings
	 * @return
	 */
	List<POJO> getListByIds(List<Serializable> ids, String... strings);

	/**
	 * 根据字段值列表查询
	 * 
	 * @param propertyName
	 * @param vls
	 * @param cls
	 * @return
	 */
	List<POJO> getList(String propertyName, List<Serializable> vls, String... cls);

	/**
	 * 根据条件分页排序查询
	 * 
	 * @param curPage
	 * @param pageSize
	 * @param orderbys
	 * @param pms
	 * @param cls
	 * @return
	 */
	List<POJO> getList(int curPage, int pageSize, LinkedHashSet<ObData> orderbys, Set<Param> pms, String... cls);

	/**
	 * 分组查询分页列表
	 * 
	 * @param curPage
	 * @param pageSize
	 * @param orderbys
	 * @param pms
	 * @param funs
	 *            <函数,属性>
	 * @param groupby
	 * @return
	 */
	List<Object[]> getGroupList(int curPage, int pageSize, LinkedHashSet<ObData> orderbys, Set<Param> pms,
			LinkedHashMap<String, String> funs, String... groupby);

	/**
	 * 分组查询总记录数
	 * 
	 * @param pms
	 * @param groupby
	 * @return
	 */
	Long getGroupbyCount(Set<Param> pms, String... groupby);

	/**
	 * 批量保存
	 * 
	 * @param pojos
	 * @return
	 */
	Integer saveList(List<POJO> pojos);

	/**
	 * 获取分页列表
	 * 
	 * @param curPage
	 * @param pageSize
	 * @param orderbys
	 * @param params
	 * @param strings
	 * @return
	 */
	PageData<POJO> getPageInfo(int curPage, int pageSize, LinkedHashSet<ObData> orderbys, Set<Param> params,
			String... strings);

	/***
	 * 分组分页
	 * 
	 * @param curPage
	 * @param pageSize
	 * @param orderbys
	 *            排序需要包含在返回的数据之内
	 * @param pms
	 *            查询条件
	 * @param funs
	 *            统计函数
	 * @param groupby
	 *            分组字段不能为空
	 * @return 函数在前分组字段在后
	 */
	PageData<Object[]> getGroupPageInfo(int curPage, int pageSize, LinkedHashSet<ObData> orderbys, Set<Param> pms,
			LinkedHashMap<String, String> funs, String... groupby);

	/**
	 * 使用统计函数
	 * 
	 * @param pms
	 *            条件
	 * @param property
	 *            属性名称
	 * @param functionName
	 *            函数名
	 * @return
	 */
	Double getStatisticsValue(Set<Param> pms, String property, StatisticsType functionName);

	/**
	 * 从主库获取数据
	 * 
	 * @param pms
	 * @param cls
	 * @return
	 */
	List<POJO> getListFromMater(Set<Param> pms, String... cls);

	/**
	 * 从主库获取数据
	 * 
	 * @param pms
	 * @return
	 */
	Long getCountFromMaster(Set<Param> pms);

	/**
	 * 从主库获取数据
	 * 
	 * @param id
	 * @param strings
	 * @return
	 */
	POJO getByIdFromMaster(Serializable id, String... strings);

	/**
	 * 从主库获取数据
	 * 
	 * @param propertyName
	 * @param value
	 * @param cls
	 * @return
	 */
	POJO getByMaster(String propertyName, Serializable value, String... cls);

	/**
	 * 从主库获取数据
	 * 
	 * @param curPage
	 * @param pageSize
	 * @param orderbys
	 * @param params
	 * @param strings
	 * @return
	 */
	PageData<POJO> getPageInfoFromMaster(int curPage, int pageSize, LinkedHashSet<ObData> orderbys, Set<Param> params,
			String... strings);

	/**
	 * 从主库获取数据
	 * 
	 * @param curPage
	 * @param pageSize
	 * @param orderbys
	 * @param pms
	 * @param funs
	 * @param groupby
	 * @return
	 */
	List<Object[]> getGroupListFromMaster(int curPage, int pageSize, LinkedHashSet<ObData> orderbys, Set<Param> pms,
			LinkedHashMap<String, String> funs, String... groupby);

	/**
	 * 从主库获取数据
	 * 
	 * @param curPage
	 * @param pageSize
	 * @param orderbys
	 * @param pms
	 * @param cls
	 * @return
	 */
	List<POJO> getListFromMaster(int curPage, int pageSize, LinkedHashSet<ObData> orderbys, Set<Param> pms,
			String... cls);

	void refreshCurrentTables();

	/**
	 * 不排序分页查询集合数据 性能最好
	 * 
	 * @param pms
	 * @param curPage
	 * @param pageSize
	 * @param cls
	 * @return
	 */
	List<POJO> getListFromMaster(Set<Param> pms, int curPage, int pageSize, String... cls);

	/**
	 * 不排序分页查询集合数据 性能最好
	 * 
	 * @param pms
	 * @param curPage
	 * @param pageSize
	 * @param cls
	 * @return
	 */
	List<POJO> getList(Set<Param> pms, int curPage, int pageSize, String... cls);

	/**
	 * 分页不排序 性能高，速度快
	 * 
	 * @param pms
	 * @param curPage
	 * @param pageSize
	 * @param cls
	 * @return
	 */
	PageData<POJO> getPageInfoFromMaster(Set<Param> pms, int curPage, int pageSize, String... cls);

	/**
	 * 分页不排序 性能高，速度快
	 * 
	 * @param pms
	 * @param curPage
	 * @param pageSize
	 * @param cls
	 * @return
	 */
	PageData<POJO> getPageInfo(Set<Param> pms, int curPage, int pageSize, String... cls);

	/**
	 * 获取属性值列表
	 * 
	 * @param property
	 * @param params
	 * @return
	 */
	List<Object> getVlList(String property, Set<Param> params);

	/**
	 * 获取属性值列表
	 * 
	 * @param property
	 * @param params
	 * @return
	 */
	List<Object> getVlListFromMaster(String property, Set<Param> params);
}
