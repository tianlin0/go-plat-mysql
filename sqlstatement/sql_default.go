package sqlstatement

import (
	"github.com/tianlin0/go-plat-utils/utils"
)

var (
	defaultStatement = new(Statement)
)

// GetAllFieldColumnsByStruct 通过对象获取所有数据库字段名
func GetAllFieldColumnsByStruct(data interface{}, tagNames ...string) ([]string, error) {
	return utils.GetFieldNamesByTag(data, tagNames...)
}

// InsertSql 插入的sql语句
func InsertSql(tableName string, allColumns []string, insertMap map[string]interface{}) (string, []interface{}) {
	return defaultStatement.InsertSql(tableName, allColumns, insertMap)
}

// UpdateSql 更新的sql语句
func UpdateSql(tableName string, allColumns []string, updateMap map[string]interface{}, whereMap map[string]interface{}) (string, []interface{}) {
	return defaultStatement.UpdateSql(tableName, allColumns, updateMap, whereMap)
}

// UpdateSqlByWhereCondition 更新的sql语句
func UpdateSqlByWhereCondition(tableName string, allColumns []string, updateMap map[string]interface{}, whereCondition SqlLogicCondition) (string, []interface{}) {
	return defaultStatement.UpdateSqlByWhereCondition(tableName, allColumns, updateMap, whereCondition)
}

// SelectSql 查询的sql语句
func SelectSql(tableName string, allColumns []string, selectStr string, whereMap map[string]interface{}, offset, num int) (string, []interface{}) {
	return defaultStatement.SelectSql(tableName, allColumns, selectStr, whereMap, offset, num)
}

// SelectSqlByWhereCondition 查询的sql语句
func SelectSqlByWhereCondition(tableName string, allColumns []string, selectStr string, whereCondition SqlLogicCondition, offset, num int) (string, []interface{}) {
	return defaultStatement.SelectSqlByWhereCondition(tableName, allColumns, selectStr, whereCondition, offset, num)
}

// DeleteSql 删除的sql语句
func DeleteSql(tableName string, allColumns []string, whereMap map[string]interface{}) (string, []interface{}) {
	return defaultStatement.DeleteSql(tableName, allColumns, whereMap)
}

// DeleteSqlByWhereCondition 删除的sql语句
func DeleteSqlByWhereCondition(tableName string, allColumns []string, whereCondition SqlLogicCondition) (string, []interface{}) {
	return defaultStatement.DeleteSqlByWhereCondition(tableName, allColumns, whereCondition)
}
