package query

import (
	"errors"
	"fmt"
	"strings"

	"github.com/singhanubhavme/in-memory-db-from-scratch-go/pkg/types"
)

var validOperators = []string{"$and", "$or", "$nor"}

func isValidOperator(operator string) bool {
	for _, validOperator := range validOperators {
		if operator == validOperator {
			return true
		}
	}
	return false
}

func Match(doc types.Document, query types.Document) (bool, error) {

	for key := range query {
		if isValidOperator(key) {
			expressions, ok := query[key].([]types.Document)
			if !ok {
				return false, fmt.Errorf("expected []types.Document for key %s", key)
			}
			match, err := handleLogicalOperators(key, doc, expressions)
			if err != nil || !match {
				return false, err
			}
			return false, nil
		} else if queryValueMap, ok := query[key].(map[string]interface{}); ok {
			fieldValue := getNestedValue(doc, key)
			match, err := handleFieldOperators(fieldValue, queryValueMap)
			if err != nil || !match {
				return false, nil
			}

		} else {
			fieldValue := getNestedValue(doc, key)
			if fieldValue != query[key] {
				return false, nil
			}
		}

	}
	return true, nil
}

func getNestedValue(doc types.Document, path string) interface{} {
	keys := strings.Split(path, ".")
	var value interface{} = doc
	for _, key := range keys {
		if m, ok := value.(map[string]interface{}); ok {
			value = m[key]
		} else {
			return nil
		}
	}
	return value
}

func handleLogicalOperators(operator string, doc types.Document, expressions []types.Document) (bool, error) {
	switch operator {
	case "$and":
		for _, expr := range expressions {
			match, err := Match(doc, expr)
			if err != nil || !match {
				return false, nil
			}
		}
		return true, nil

	case "$or":
		for _, expr := range expressions {
			match, err := Match(doc, expr)
			if err != nil || match {
				return true, nil
			}
		}
		return false, nil

	case "$nor":
		for _, expr := range expressions {
			match, err := Match(doc, expr)
			if err != nil || match {
				return false, nil
			}
		}
		return true, nil

	default:
		return false, fmt.Errorf("unknown logical operator: %s", operator)
	}
}

func handleFieldOperators(value interface{}, condition map[string]interface{}) (bool, error) {
	for op, cond := range condition {
		operatorFunc, exists := GetOperator(op)
		if !exists {
			return false, errors.New("unknown operator: " + op)
		}

		match, err := operatorFunc(value, cond)
		if err != nil || !match {
			return false, nil
		}
	}
	return true, nil

}
