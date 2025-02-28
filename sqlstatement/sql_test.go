package sqlstatement

import (
	"fmt"
	"testing"
)

func TestGenerateWhereClause(t *testing.T) {
	sta := new(Statement)

	sqlStr, list := sta.GenerateWhereClause(SqlLogicCondition{
		Conditions: []interface{}{
			SqlCondition{
				Field:    "name",
				Value:    "test",
				Operator: "=",
			},
			SqlCondition{
				Field:    "age",
				Value:    18,
				Operator: "=",
			},
			SqlLogicCondition{
				Conditions: []interface{}{
					SqlCondition{
						Field:    "name",
						Value:    "test",
						Operator: "=",
					},
					SqlCondition{
						Field:    "age",
						Value:    18,
						Operator: "=",
					},
				},
				Operator: "or",
			},
		},
		Operator: "and",
	})

	fmt.Println(sqlStr, list)

	sqlStr, list = sta.GenerateWhereClause(SqlLogicCondition{
		Conditions: []interface{}{
			SqlLogicCondition{
				Conditions: []interface{}{
					SqlCondition{
						Field:    "name",
						Value:    "test",
						Operator: "=",
					},
					SqlCondition{
						Field:    "age",
						Value:    18,
						Operator: "=",
					},
				},
				Operator: "and",
			},
			SqlLogicCondition{
				Conditions: []interface{}{
					SqlCondition{
						Field:    "name",
						Value:    "test",
						Operator: "=",
					},
					SqlCondition{
						Field:    "age",
						Value:    18,
						Operator: "=",
					},
				},
				Operator: "or",
			},
		},
		Operator: "or",
	})

	fmt.Println(sqlStr, list)

	sqlStr, list = sta.GenerateWhereClause(SqlLogicCondition{
		Conditions: []interface{}{
			SqlCondition{
				Field:    "name",
				Value:    "test",
				Operator: "=",
			},
		},
		Operator: "or",
	})

	fmt.Println(sqlStr, list)

}
