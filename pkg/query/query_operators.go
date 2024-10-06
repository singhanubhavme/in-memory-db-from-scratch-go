package query

import "regexp"

type OperatorFunction func(fieldValue interface{}, queryValue interface{}) (bool, error)

var operators = map[string]OperatorFunction{
	"$eq": func(fieldValue, queryValue interface{}) (bool, error) {
		return fieldValue == queryValue, nil
	},
	"$ne": func(fieldValue, queryValue interface{}) (bool, error) {
		return fieldValue != queryValue, nil
	},
	"$gt": func(fieldValue, queryValue interface{}) (bool, error) {
		return fieldValue.(int) > queryValue.(int), nil
	},
	"$lt": func(fieldValue, queryValue interface{}) (bool, error) {
		return fieldValue.(int) < queryValue.(int), nil
	},
	"$gte": func(fieldValue, queryValue interface{}) (bool, error) {
		return fieldValue.(int) >= queryValue.(int), nil
	},
	"$lte": func(fieldValue, queryValue interface{}) (bool, error) {
		return fieldValue.(int) <= queryValue.(int), nil
	},
	"$in": func(fieldValue, queryValue interface{}) (bool, error) {
		for _, v := range queryValue.([]interface{}) {
			if fieldValue == v {
				return true, nil
			}
		}
		return false, nil
	},
	"$nin": func(fieldValue, queryValue interface{}) (bool, error) {
		for _, v := range queryValue.([]interface{}) {
			if fieldValue == v {
				return false, nil
			}
		}
		return true, nil
	},
	"$exists": func(fieldValue, queryValue interface{}) (bool, error) {
		return queryValue.(bool), nil
	},
	"$regex": func(fieldValue, queryValue interface{}) (bool, error) {
		if str, ok := fieldValue.(string); ok {
			regex, err := regexp.Compile(queryValue.(string))
			if err != nil {
				return false, err
			}
			return regex.MatchString(str), nil
		}
		return false, nil
	},
}

func GetOperator(name string) (OperatorFunction, bool) {
	op, exists := operators[name]
	return op, exists
}
