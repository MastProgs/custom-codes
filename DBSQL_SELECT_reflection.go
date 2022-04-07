package main

import (
	"database/sql"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type tblaccount struct {
	PlayerKey   string
	UserUUID    int64
	ConnectIP   string
	ConnectTime time.Time
	CreateTime  time.Time
	GameDBID    int
}

const (
	SQL_SELECT = iota + 100
	SQL_INSERT
	SQL_UPDATE
	SQL_DELETE

	DB_UNUSE_STRING = "\t\n0"
)

func DB_IsUse(val reflect.Value) bool {
	v := val
	k := v.Kind()

	switch k {
	case reflect.String:
		return v.String() != DB_UNUSE_STRING

	case reflect.Int:
		return v.Int() != math.MaxInt
	case reflect.Int8:
		return v.Int() != math.MaxInt8
	case reflect.Int16:
		return v.Int() != math.MaxInt16
	case reflect.Int32:
		return v.Int() != math.MaxInt32
	case reflect.Int64:
		return v.Int() != math.MaxInt64
	case reflect.Uint:
		return v.Uint() != math.MaxUint
	case reflect.Uint8:
		return v.Uint() != math.MaxUint8
	case reflect.Uint16:
		return v.Uint() != math.MaxUint16
	case reflect.Uint32:
		return v.Uint() != math.MaxUint32
	case reflect.Uint64:
		return v.Uint() != math.MaxUint64

	case reflect.Float32, reflect.Complex64:
		return v.Float() != math.MaxFloat32
	case reflect.Float64, reflect.Complex128:
		return v.Float() != math.MaxFloat64

	}

	return false
}

func DB_ToString(val reflect.Value) string {
	v := val
	k := v.Kind()

	switch k {
	case reflect.String:
		return "\"" + v.String() + "\""

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return fmt.Sprint(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return fmt.Sprint(v.Uint())

	case reflect.Float32, reflect.Complex64, reflect.Float64, reflect.Complex128:
		return fmt.Sprint(v.Float())
	}

	return "0"
}

func DB_InitTable(tbls ...interface{}) {

	for _, t := range tbls {

		tbl_val := reflect.ValueOf(t).Elem()
		tbl_type := reflect.TypeOf(t).Elem()

		for i := 0; i < tbl_val.NumField(); i++ {
			v := tbl_val.Field(i)
			t := tbl_type.Field(i)

			switch t.Type.Kind() {

			case reflect.String:
				v.SetString(DB_UNUSE_STRING)

			case reflect.Int:
				v.SetInt(math.MaxInt)
			case reflect.Int8:
				v.SetInt(math.MaxInt8)
			case reflect.Int16:
				v.SetInt(math.MaxInt16)
			case reflect.Int32:
				v.SetInt(math.MaxInt32)
			case reflect.Int64:
				v.SetInt(math.MaxInt64)
			case reflect.Uint:
				v.SetUint(math.MaxUint)
			case reflect.Uint8:
				v.SetUint(math.MaxUint8)
			case reflect.Uint16:
				v.SetUint(math.MaxUint16)
			case reflect.Uint32:
				v.SetUint(math.MaxUint32)
			case reflect.Uint64:
				v.SetUint(math.MaxUint64)

			case reflect.Float32, reflect.Complex64:
				v.SetFloat(math.MaxFloat32)
			case reflect.Float64, reflect.Complex128:
				v.SetFloat(math.MaxFloat64)

			}
		}
	}

}

func DB_SetValue(param reflect.Value, set_value reflect.Value) {
	v := param
	k := v.Kind()

	switch k {

	case reflect.String:
		v.SetString(set_value.String())

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(set_value.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(set_value.Uint())

	case reflect.Float32, reflect.Complex64, reflect.Float64, reflect.Complex128:
		v.SetFloat(set_value.Float())
	}
}

/*
	< How To Use >
	ex)
		type tblaccount struct {
			PlayerKey   string
			UserUUID    int64
			ConnectIP   string
			ConnectTime time.Time
			CreateTime  time.Time
			GameDBID    int
		}

		var tbl_select, tbl_where tblaccount
		DB_InitTable(&tbl_select, &tbl_where)

		tbl_select.PlayerKey = "Yea"	<- It doesn't matter what value you set
		tbl_select.GameDBID = 9999		<- It doesn't matter what value you set

		tbl_where.UserUUID = 10			<- Setting value is important in setting query conditions
		tbl_where.GameDBID = 1			<- Setting value is important in setting query conditions
		tbl_where.UserUUID = 10			<- Setting value is important in setting query conditions

		DB_SELECT(SQL_SELECT, &tbl_select, &tbl_where)	<- Sended Query : SELECT PlayerKey, GameDBID FROM tblaccount WHERE UserUUID = 10 AND ConnectIP = "127.0.0.1" AND GameDBID = 1
*/
func DB_SELECT(db *sql.DB, sql_cmd_type int, tbl_columns interface{}, tbl_where interface{}) error {

	/*
		Check that each table type is the same.
		각 테이블 타입이 동일한지 체크.
	*/
	{
		tb_col_t := reflect.TypeOf(tbl_columns)
		tb_where_t := reflect.TypeOf(tbl_where)

		if tb_col_t.Name() != tb_where_t.Name() {
			return errors.New(fmt.Sprint("[ SQL ERROR ] SQL Table Not Same -", tb_col_t.Elem().Name(), ":", tb_where_t.Elem().Name()))
		}
	}

	from_table := reflect.TypeOf(tbl_columns).Elem().Name()

	/*
		Extract the columns that will be affected from the SELECT UPDATE INSERT syntax.
		SELECT UPDATE INSERT 구문에서 영향 받을 컬럼들부터 추출.
	*/
	var target_column []string
	var target_index []int

	tbl_val := reflect.ValueOf(tbl_columns).Elem()
	tbl_type := reflect.TypeOf(tbl_columns).Elem()

	for i := 0; i < tbl_val.NumField(); i++ {
		t := tbl_type.Field(i)

		if true == DB_IsUse(tbl_val.Field(i)) {
			target_index = append(target_index, i)
			target_column = append(target_column, t.Name)
		}
	}

	/*
		Extract columns and values to be conditioned in WHERE clause.
		WHERE 절의 조건이 될 컬럼과 값을 추출.
	*/
	var where_column []string
	var where_index []int
	var where_val []reflect.Value

	tbl_where_val := reflect.ValueOf(tbl_where).Elem()
	tbl_where_type := reflect.TypeOf(tbl_where).Elem()

	if 0 == tbl_where_val.NumField() {
		return errors.New("[ SQL ERROR ] There is no SQL WHERE column value.")
	}

	for i := 0; i < tbl_where_val.NumField(); i++ {
		t := tbl_where_type.Field(i)

		val := tbl_where_val.Field(i)
		if true == DB_IsUse(val) {
			where_index = append(where_index, i)
			where_column = append(where_column, t.Name)
			where_val = append(where_val, val)
		}
	}

	/*
		Set table member var address parameters to receive results.
		결과 받을 테이블 주소 파라미터 셋팅.
	*/
	var target_ptr_list []interface{}

	for _, d := range target_index {
		target_ptr_list = append(target_ptr_list, tbl_val.Field(d).Addr().Interface())
	}

	/*
		Make Query.
		쿼리 생성.
	*/

	var queryWhereElems []string
	for i := 0; i < len(where_column); i++ {
		queryWhere := ""
		queryWhere += where_column[i]
		queryWhere += " = "
		queryWhere += DB_ToString(where_val[i])
		queryWhereElems = append(queryWhereElems, queryWhere)
	}

	queryStr := "SELECT " + strings.Join(target_column, ", ") + " FROM " + from_table + " WHERE " + strings.Join(queryWhereElems, " AND ")
	fmt.Println(queryStr)

	/*
		Send Query.
		쿼리 요청.
	*/
	err := db.QueryRow(queryStr).Scan(target_ptr_list...)
	if err != nil {
		fmt.Println(err)
	}

	return nil
}

func main() {

	db, err := sql.Open("mysql", "...")
	if err != nil {
		fmt.Println(err)
	}
	defer db.Close()

	var tbl_select, tbl_where tblaccount
	DB_InitTable(&tbl_select, &tbl_where)

	tbl_select.PlayerKey = "Yea"
	tbl_select.GameDBID = 9999

	tbl_where.ConnectIP = "127.0.0.1"
	tbl_where.GameDBID = 1
	tbl_where.UserUUID = 10

	DB_SELECT(db, SQL_SELECT, &tbl_select, &tbl_where)

	fmt.Println(tbl_select.PlayerKey, &tbl_select.PlayerKey)
	fmt.Println(tbl_select.GameDBID, &tbl_select.GameDBID)
}
