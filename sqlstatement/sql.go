package sqlstatement

import (
	"fmt"
	"github.com/samber/lo"
	"github.com/tianlin0/go-plat-utils/cond"
	"github.com/tianlin0/go-plat-utils/conv"
	"github.com/tianlin0/go-plat-utils/utils"
	"reflect"
	"strings"
)

// SqlCondition 表示单个查询条件
type SqlCondition struct {
	Field    string
	Operator string
	Value    interface{}
}

// SqlLogicCondition 表示逻辑分组
type SqlLogicCondition struct {
	Conditions []interface{} // 可以是 SqlCondition 或 SqlLogicCondition
	Operator   string        // "AND" 或 "OR"
}

var (
	operatorList       = []string{"LIKE", "=", ">=", ">", "<=", "<"} // 数据库支持的类型
	likeUseReplaceList = []string{"%", "_"}                          //like需要替换的字符
	likeUseEscapeList  = []string{"/", "&", "#", "@", "^", "$", "!"} //定义可以使用的escape列表
)

type Statement struct {
}

func (s *Statement) getColumnLikeSql(oldValue string, replaceList []string, escapeList []string) (retValLike string, retEscape string, retSuccess bool) {
	isFind := false
	for _, one := range replaceList {
		if in := strings.IndexAny(oldValue, one); in >= 0 {
			isFind = true
		}
	}

	if !isFind {
		return oldValue, "", true
	}

	oneEscapeStr := ""
	for _, one := range escapeList {
		if in := strings.IndexAny(oldValue, one); in < 0 {
			//不存在，则可作为转义符
			oneEscapeStr = one
			break
		}
	}

	if oneEscapeStr == "" {
		return oldValue, "", false
	}

	for _, one := range replaceList {
		oldValue = strings.ReplaceAll(oldValue, one, oneEscapeStr+one)
	}

	return oldValue, oneEscapeStr, true
}

// getSqlColumnForLike 获取列名转义sql
func (s *Statement) getSqlColumnForLike(oldValue string) (retValLike string, retParam string) {
	newValue, escape, retTrue := s.getColumnLikeSql(oldValue, likeUseReplaceList, likeUseEscapeList)
	if retTrue {
		if escape != "" {
			return "? escape '" + escape + "'", newValue
		}
	}

	return "?", newValue
}

// GenerateWhereClauseByMap 通过Map获取where语句
func (s *Statement) GenerateWhereClauseByMap(whereMap map[string]interface{}) (string, []interface{}) {
	oneLogicCondition := SqlLogicCondition{
		Conditions: make([]interface{}, 0),
		Operator:   "AND",
	}
	for key, val := range whereMap {
		oneCondition := SqlCondition{
			Field:    key,
			Operator: "=",
			Value:    val,
		}

		s := reflect.ValueOf(val)
		if one, ok := s.Interface().(SqlCondition); ok {
			oneCondition.Operator = one.Operator
			oneCondition.Value = one.Value
		}
		oneLogicCondition.Conditions = append(oneLogicCondition.Conditions, oneCondition)
	}
	return s.GenerateWhereClause(oneLogicCondition)
}

// GenerateWhereClause 生成 WHERE 语句
func (s *Statement) GenerateWhereClause(group SqlLogicCondition) (string, []interface{}) {
	if group.Operator == "" {
		group.Operator = "AND"
	}

	group.Operator = strings.ToUpper(group.Operator)

	var parts []string
	dataList := make([]interface{}, 0)
	for _, cond := range group.Conditions {
		switch c := cond.(type) {
		case SqlCondition:
			sqlStr, tempDataList := s.generateWhereFromCondition(c)
			if sqlStr != "" {
				parts = append(parts, fmt.Sprintf("(%s)", sqlStr))
				dataList = append(dataList, tempDataList...)
			}
			continue
		case SqlLogicCondition:
			sqlStr, tempDataList := s.GenerateWhereClause(c)
			if sqlStr != "" {
				parts = append(parts, fmt.Sprintf("(%s)", sqlStr))
				dataList = append(dataList, tempDataList...)
			}
			continue
		}
	}
	if len(parts) == 0 {
		return "", dataList
	}
	return strings.Join(parts, fmt.Sprintf(" %s ", group.Operator)), dataList
}

// generateWhereClause 生成 WHERE 语句
func (s *Statement) generateWhereFromCondition(con SqlCondition) (string, []interface{}) {
	con.Operator = strings.ToUpper(con.Operator)

	//如果val是数组，则operator只能是in
	if reflect.TypeOf(con.Value).Kind() == reflect.Slice {
		s := reflect.ValueOf(con.Value)
		//需要去重处理
		paramList := make([]string, 0)
		dataList := make([]interface{}, 0)
		onlyArray := make([]string, 0)
		for i := 0; i < s.Len(); i++ {
			ele := s.Index(i).Interface()
			tempOne := conv.String(ele)
			if ret, _ := cond.Contains(onlyArray, tempOne); !ret {
				onlyArray = utils.AppendUniq(onlyArray, tempOne)
				paramList = append(paramList, "?")
				dataList = append(dataList, ele)
			}
		}
		if len(dataList) > 0 {
			//只能为IN，NOT IN
			opt := "IN"
			if con.Operator != "" && con.Operator == "NOT IN" {
				opt = con.Operator
			}
			return fmt.Sprintf("`%s` %s (%s)", con.Field, opt, strings.Join(paramList, ",")), dataList
		}
		return "", []interface{}{}
	}

	if con.Operator == "" {
		con.Operator = "="
	}

	//必须是支持的类型，乱传不支持的类型则跳过
	if ok, _ := cond.Contains(operatorList, con.Operator); !ok {
		return "", []interface{}{}
	}

	if con.Operator == "LIKE" {
		// 这里需要对value进行特殊处理
		valLike, newVal := s.getSqlColumnForLike(conv.String(con.Value))
		return fmt.Sprintf("`%s` %s %s", con.Field, con.Operator, valLike), []interface{}{newVal}
	}

	return fmt.Sprintf("`%s` %s ?", con.Field, con.Operator), []interface{}{con.Value}
}

// buildFieldNames 需要将 `name` 转为 name
func (s *Statement) buildFieldNames(canUpdateFieldNames []string) []string {
	canUpdateFieldNamesTemp := make([]string, 0)
	lo.ForEach(canUpdateFieldNames, func(item string, index int) {
		canUpdateFieldNamesTemp = append(canUpdateFieldNamesTemp, strings.ReplaceAll(item, "`", ""))
	})
	return canUpdateFieldNamesTemp
}

// getColumnListAndDataList 获取数据库的字段列表与数据
func (s *Statement) getColumnListAndDataList(fieldNames []string, columnMap map[string]interface{}) ([]string, []interface{}) {
	fieldNamesTemp := s.buildFieldNames(fieldNames)

	columnList := make([]string, 0)
	// 必须检查是数据库的字段名，避免传错的名字
	columns := lo.Keys(columnMap)
	lo.ForEach(columns, func(column string, index int) {
		if lo.IndexOf(fieldNamesTemp, column) >= 0 {
			columnList = append(columnList, column)
		}
	})
	columnDataList := make([]interface{}, 0, len(columnList))

	if len(columnList) == 0 {
		return columnList, columnDataList
	}
	lo.ForEach(columnList, func(column string, index int) {
		columnDataList = append(columnDataList, columnMap[column])
	})
	return columnList, columnDataList
}

// InsertSql 插入的sql语句
func (s *Statement) InsertSql(tableName string, allColumns []string, insertMap map[string]interface{}) (string, []interface{}) {
	columnList, columnDataList := s.getColumnListAndDataList(allColumns, insertMap)
	if len(columnList) == 0 {
		return "", columnDataList
	}
	query := fmt.Sprintf("INSERT INTO `%s` set (%s)", tableName, strings.Join(columnList, "=?,")+"=?")
	return query, columnDataList
}

// UpdateSql 更新的sql语句
func (s *Statement) UpdateSql(tableName string, allColumns []string, updateMap map[string]interface{}, whereMap map[string]interface{}) (string, []interface{}) {
	columnList, columnDataList := s.getColumnListAndDataList(allColumns, updateMap)
	if len(columnList) == 0 {
		return "", columnDataList
	}

	//过滤key
	whereNewMap := make(map[string]interface{})
	for k, v := range whereMap {
		if lo.IndexOf(allColumns, k) >= 0 {
			whereNewMap[k] = v
		}
	}

	whereString, whereDataList := s.GenerateWhereClauseByMap(whereNewMap)
	if len(whereString) == 0 {
		//没有where语句
		query := fmt.Sprintf("UPDATE `%s` set (%s)", tableName, strings.Join(columnList, "=?,")+"=?")
		return query, columnDataList
	}
	columnDataList = append(columnDataList, whereDataList...)
	query := fmt.Sprintf("UPDATE `%s` set (%s) WHERE %s", tableName, strings.Join(columnList, "=?,")+"=?", whereString)
	return query, columnDataList
}

// UpdateSqlByWhereCondition 更新的sql语句
func (s *Statement) UpdateSqlByWhereCondition(tableName string, allColumns []string, updateMap map[string]interface{}, whereCondition SqlLogicCondition) (string, []interface{}) {
	query, updateColumnDataList := s.UpdateSql(tableName, allColumns, updateMap, map[string]interface{}{})
	whereStr, whereDataList := s.GenerateWhereClause(whereCondition)
	if whereStr == "" {
		return query, updateColumnDataList
	}
	query = fmt.Sprintf("%s WHERE %s", query, whereStr)
	updateColumnDataList = append(updateColumnDataList, whereDataList...)
	return query, updateColumnDataList
}

// SelectSql 查询的sql语句
func (s *Statement) SelectSql(tableName string, allColumns []string, selectStr string, whereMap map[string]interface{}, offset, num int) (string, []interface{}) {
	if selectStr == "" {
		selectStr = "*"
	} else {
		selectList := strings.Split(selectStr, ",")
		newSelectList := make([]string, 0)
		lo.ForEach(selectList, func(item string, index int) {
			item = strings.TrimSpace(item)
			if lo.IndexOf(allColumns, item) >= 0 {
				newSelectList = append(newSelectList, item)
			}
		})
		if len(newSelectList) > 0 {
			selectStr = strings.Join(newSelectList, "`, `")
			selectStr = fmt.Sprintf("`%s`", selectStr)
		} else {
			//表示要查询 count(*) 等，就不用管了
		}
	}

	//过滤key
	whereNewMap := make(map[string]interface{})
	for k, v := range whereMap {
		if lo.IndexOf(allColumns, k) >= 0 {
			whereNewMap[k] = v
		}
	}

	whereString, whereDataList := s.GenerateWhereClauseByMap(whereNewMap)
	query := fmt.Sprintf("SELECT %s FROME `%s`", selectStr, tableName)
	if whereString != "" {
		query = fmt.Sprintf("%s WHERE %s", query, whereString)
	}
	if offset >= 0 && num > 0 {
		query = fmt.Sprintf("%s LIMIT %d, %d", query, offset, num)
	}

	return query, whereDataList
}

// SelectSqlByWhereCondition 查询的sql语句
func (s *Statement) SelectSqlByWhereCondition(tableName string, allColumns []string, selectStr string, whereCondition SqlLogicCondition, offset, num int) (string, []interface{}) {
	query, selectDataList := s.SelectSql(tableName, allColumns, selectStr, map[string]interface{}{}, 0, 0)
	whereStr, whereDataList := s.GenerateWhereClause(whereCondition)
	if whereStr == "" {
		return query, selectDataList
	}
	query = fmt.Sprintf("%s WHERE %s", query, whereStr)
	if offset >= 0 && num > 0 {
		query = fmt.Sprintf("%s LIMIT %d, %d", query, offset, num)
	}
	selectDataList = append(selectDataList, whereDataList...)
	return query, selectDataList
}

// DeleteSql 删除的sql语句
func (s *Statement) DeleteSql(tableName string, allColumns []string, whereMap map[string]interface{}) (string, []interface{}) {
	//过滤key
	whereNewMap := make(map[string]interface{})
	for k, v := range whereMap {
		if lo.IndexOf(allColumns, k) >= 0 {
			whereNewMap[k] = v
		}
	}
	whereString, whereDataList := s.GenerateWhereClauseByMap(whereNewMap)
	query := fmt.Sprintf("DELETE FROME `%s`", tableName)
	if whereString != "" {
		query = fmt.Sprintf("%s WHERE %s", query, whereString)
	}
	return query, whereDataList
}

// DeleteSqlByWhereCondition 删除的sql语句
func (s *Statement) DeleteSqlByWhereCondition(tableName string, allColumns []string, whereCondition SqlLogicCondition) (string, []interface{}) {
	query, deleteDataList := s.DeleteSql(tableName, allColumns, map[string]interface{}{})
	whereStr, whereDataList := s.GenerateWhereClause(whereCondition)
	if whereStr == "" {
		return query, deleteDataList
	}
	query = fmt.Sprintf("%s WHERE %s", query, whereStr)
	deleteDataList = append(deleteDataList, whereDataList...)
	return query, deleteDataList
}
