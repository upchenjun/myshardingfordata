package com.fd.myshardingfordata.helper;

import javax.persistence.EnumType;

import com.fd.myshardingfordata.annotation.ColumnRule;
import com.fd.myshardingfordata.annotation.MyIndex;

/**
 * 字段映射信息
 * 
 * @author 符冬
 *
 */
public class PropInfo {
	// 属性名称
	private String pname;
	// 数据库字段名称
	private String cname;
	// 是否主键
	private Boolean isPrimarykey = false;
	// 如果是切分字段，包含切分数据配置
	private ColumnRule columnRule;
	// 属性类型
	private Class<?> type;
	// java.sql.Types,数据库字段类型
	private Integer sqlTypes;
	// 是否大字段
	private Boolean isLob = false;
	// 字段长度
	private Integer length = 255;
	// 是否不为空
	private Boolean isNotNull = false;
	// 是否唯一
	private Boolean isUnique = false;
	// 主键是否自动增长
	private Boolean autoIncreament = false;
	// 创建索引信息
	private MyIndex index;
	// 枚举映射数据库的类型
	private EnumType enumType;

	public String getPname() {
		return pname;
	}

	public MyIndex getIndex() {
		return index;
	}

	public void setIndex(MyIndex index) {
		this.index = index;
	}

	public Boolean getAutoIncreament() {
		return autoIncreament;
	}

	public void setAutoIncreament(Boolean autoIncreament) {
		this.autoIncreament = autoIncreament;
	}

	public EnumType getEnumType() {
		return enumType;
	}

	public void setEnumType(EnumType enumType) {
		this.enumType = enumType;
	}

	public Boolean getIsLob() {
		return isLob;
	}

	public Boolean getIsUnique() {
		return isUnique;
	}

	public void setIsUnique(Boolean isUnique) {
		this.isUnique = isUnique;
	}

	public Boolean getIsNotNull() {
		return isNotNull;
	}

	public void setIsNotNull(Boolean isNotNull) {
		this.isNotNull = isNotNull;
	}

	public void setIsLob(Boolean isLob) {
		this.isLob = isLob;
	}

	public Integer getLength() {
		return length;
	}

	public void setLength(Integer length) {
		this.length = length;
	}

	public PropInfo(String pname, Class<?> type) {
		super();
		this.pname = pname;
		this.type = type;
	}

	public Class<?> getType() {
		return type;
	}

	public void setType(Class<?> type) {
		this.type = type;
	}

	public void setPname(String pname) {
		this.pname = pname;
	}

	public PropInfo() {
		super();
	}

	public String getCname() {
		return cname;
	}

	public void setCname(String cname) {
		this.cname = cname;
	}

	public Boolean getIsPrimarykey() {
		return isPrimarykey;
	}

	public void setIsPrimarykey(Boolean isPrimarykey) {
		this.isPrimarykey = isPrimarykey;
	}

	public ColumnRule getColumnRule() {
		return columnRule;
	}

	public void setColumnRule(ColumnRule columnRule) {
		this.columnRule = columnRule;
	}

	public Integer getSqlTypes() {
		return sqlTypes;
	}

	public void setSqlTypes(Integer sqlTypes) {
		this.sqlTypes = sqlTypes;
	}

}
