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
	SQL_UPSERT
	SQL_MERGE
	SQL_DELETE
	SQL_INCRESE
	SQL_DECRESE

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

	case reflect.TypeOf(time.Time{}).Kind():
		return v.Interface().(time.Time).Format("2006-01-02 15:04:05") != time.Time{}.Format("2006-01-02 15:04:05") //time.RFC3339Nano)
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

	case reflect.TypeOf(time.Time{}).Kind():
		return "\"" + v.Interface().(time.Time).Format("2006-01-02 15:04:05") + "\"" //time.RFC3339Nano)
	}

	return ""
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

func DB_Make_SELECT_Query(tbl_columns interface{}, tbl_where interface{}, raw_where ...string) (string, error) {

	from_table := reflect.TypeOf(tbl_columns).Elem().Name()

	/*
		Extract the columns that will be affected from the SELECT UPDATE INSERT syntax.
		SELECT UPDATE INSERT 구문에서 영향 받을 컬럼들부터 추출.
	*/
	var target_column []string

	tbl_val := reflect.ValueOf(tbl_columns).Elem()
	tbl_type := reflect.TypeOf(tbl_columns).Elem()

	for i := 0; i < tbl_val.NumField(); i++ {
		t := tbl_type.Field(i)

		if true == DB_IsUse(tbl_val.Field(i)) {
			target_column = append(target_column, t.Name)
		}
	}

	/*
		Extract columns and values to be conditioned in WHERE clause.
		WHERE 절의 조건이 될 컬럼과 값을 추출.
	*/
	var where_str string

	if 0 < len(raw_where) {
		where_str = raw_where[0]
	} else {
		var where_column []string
		var where_index []int
		var where_val []reflect.Value

		tbl_where_val := reflect.ValueOf(tbl_where).Elem()
		tbl_where_type := reflect.TypeOf(tbl_where).Elem()

		if 0 == tbl_where_val.NumField() {
			return "", errors.New("[ SQL ERROR ] There is no SQL WHERE column value.")
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
		where_str = strings.Join(queryWhereElems, " AND ")
	}

	queryStr := "SELECT " + strings.Join(target_column, ", ") + " FROM " + from_table + " WHERE " + where_str + ";"
	fmt.Println(queryStr)

	return queryStr, nil
}

func DB_MAKE_INSERT_Query[DB_Table interface{}](tbl_insert ...DB_Table) (string, error) {

	if 1 > len(tbl_insert) {
		return "", errors.New(fmt.Sprint("[ SQL ERROR ] There is no data for INSERT"))
	}

	queryStr := "INSERT INTO "

	/*
		This function optionally allows you to attempt to INSERT only a few columns.
		이 함수에서는 선택적으로 특정 몇몇 컬럼만을 INSERT 하려고 하는 행위를 허용함.
	*/
	var valid_field_index []int

	{
		/*
			Extract the table name and each column name.
			테이블 명과 각 컬럼명을 추출.
		*/
		tbl := reflect.ValueOf(&tbl_insert[0]).Elem()
		field_size := tbl.NumField()
		tbl_type := reflect.TypeOf(&tbl_insert[0]).Elem()

		var field_names []string
		for i := 0; i < field_size; i++ {
			t := tbl_type.Field(i)

			if true == DB_IsUse(tbl.Field(i)) {
				field_names = append(field_names, t.Name)
				valid_field_index = append(valid_field_index, i)
			}
		}

		/*
			Change extracted name to query string.
			추출한 명칭을 쿼리 문자열로 변경.
		*/
		queryStr += (tbl_type.Name() + " (")
		queryStr += (strings.Join(field_names, ", ") + ") VALUES ")
	}
	{
		/*
			Change each value to be INSERT query form.
			INSERT 할 각 값들을 쿼리 형태로 변경.
		*/
		var tbl_elem_array []string
		for _, tbl_elem := range tbl_insert {
			tbl := reflect.ValueOf(&tbl_elem).Elem()
			field_size := tbl.NumField()

			// elem_row_value == arr[ field1_value, field2_value, ... ]
			var elem_row_value []string
			for i := 0; i < field_size; i++ {
				val := tbl.Field(i)

				if true == DB_IsUse(val) {
					elem_row_value = append(elem_row_value, DB_ToString(val))
				}
			}

			// elem_row_value make row like strings => (field1_value, field2_value, ...)
			tbl_elem_array = append(tbl_elem_array, "("+strings.Join(elem_row_value, ", ")+")")
		}

		// queryStr's final query form => INSERT INTO tbl (col1, col2, ...) VALUES (val1, val2, ...), ... ;
		queryStr += strings.Join(tbl_elem_array, ", ") + ";"
		fmt.Println(queryStr)
	}

	return queryStr, nil
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

		tbl_select.PlayerKey = "Yea"			<- It doesn't matter what value you set
		tbl_select.GameDBID = 9999				<- It doesn't matter what value you set

		tbl_where.ConnectIP = "127.0.0.1"		<- Setting value is important in setting query conditions
		tbl_where.GameDBID = 1					<- Setting value is important in setting query conditions
		tbl_where.UserUUID = 10					<- Setting value is important in setting query conditions

		DB_SELECT(db, &tbl_select, &tbl_where)	<- Sended Query : SELECT PlayerKey, GameDBID FROM tblaccount WHERE UserUUID = 10 AND ConnectIP = "127.0.0.1" AND GameDBID = 1

		// When you enter raw_where value, tbl_where table value is ignored
		// raw_where 값을 입력 시, tbl_where 테이블 값은 무시됨
		DB_SELECT(db, tbl_select, tbl_where, "UserUUID = 10") <- Sended Query : SELECT PlayerKey, GameDBID FROM tblaccount WHERE UserUUID = 10
*/
func DB_SELECT[DB_Table interface{}](db *sql.DB, tbl_target DB_Table, tbl_where DB_Table, raw_where ...string) ([]DB_Table, error) {

	var retValues []DB_Table

	/*
		Check that each table type is the same.
		각 테이블 타입이 동일한지 체크.
	*/
	{
		tb_col_t := reflect.TypeOf(&tbl_target)
		tb_where_t := reflect.TypeOf(&tbl_where)

		if tb_col_t.Name() != tb_where_t.Name() {
			return retValues, errors.New(fmt.Sprint("[ SQL ERROR ] SQL Table Not Same -", tb_col_t.Elem().Name(), ":", tb_where_t.Elem().Name()))
		}
	}

	/*
		Save the source member variable index value to receive the value.
		값을 받을 원본 멤버 변수 인덱스 값을 저장.
	*/
	var target_index []int
	tbl_val := reflect.ValueOf(&tbl_target).Elem()
	for i := 0; i < tbl_val.NumField(); i++ {
		if true == DB_IsUse(tbl_val.Field(i)) {
			target_index = append(target_index, i)
		}
	}

	queryStr, err := DB_Make_SELECT_Query(&tbl_target, &tbl_where, raw_where...)
	if err != nil {
		fmt.Println(err)
		return retValues, err
	}

	rows, err := db.Query(queryStr)
	for rows.Next() {

		/*
			Set table member var address parameters to receive results.
			결과 받을 테이블 주소 파라미터 셋팅.
		*/
		var obj DB_Table
		retT_val := reflect.ValueOf(&obj).Elem()
		var target_ptr_list []interface{}
		for _, d := range target_index {
			target_ptr_list = append(target_ptr_list, retT_val.Field(d).Addr().Interface())
		}

		/*
			Create result values sequentially on table objects.
			테이블 객체에 순차적으로 결과 값 작성.
		*/
		err = rows.Scan(target_ptr_list...)
		if err != nil {
			fmt.Println(err)
			return retValues, err
		}

		retValues = append(retValues, obj)
	}

	return retValues, nil
}

func DB_INSERT[DB_Table interface{}](db *sql.DB, tbl_insert ...DB_Table) (int64, error) {

	if 1 > len(tbl_insert) {
		return 0, errors.New(fmt.Sprint("[ SQL ERROR ] There is no data for INSERT"))
	}

	/*
		Error handling if any of the table column values to be INSERT are abnormal.
		INSERT 할 테이블 컬럼 값이 하나라도 비정상인 경우 에러처리.
	*/
	tbl_name := reflect.TypeOf(tbl_insert[0]).Name()

	for _, tbl_in := range tbl_insert {
		tbl_val := reflect.ValueOf(&tbl_in).Elem()

		elemTbl_name := reflect.TypeOf(tbl_in).Name()
		if tbl_name != elemTbl_name {
			return 0, errors.New(fmt.Sprint("[ SQL ERROR ] Not Same tables elements in INSERT ( ", tbl_name, " <> ", elemTbl_name, " )"))
		}

		for i := 0; i < tbl_val.NumField(); i++ {
			if true != DB_IsUse(tbl_val.Field(i)) {
				return 0, errors.New(fmt.Sprint("[ SQL ERROR ] Invalid table field value - ", elemTbl_name))
			}
		}
	}

	queryStr, err := DB_MAKE_INSERT_Query(tbl_insert...)
	if err != nil {
		return 0, err
	}

	res, err := db.Exec(queryStr)
	if err != nil {
		return 0, err
	}

	affect, err := res.RowsAffected()
	if err != nil {
		fmt.Println("[ SQL ERROR ] Rows Affected error - ", err)
	}

	return affect, err
}

func main() {

	db, err := sql.Open("mysql", "conn")
	if err != nil {
		fmt.Println(err)
	}
	defer db.Close()

	// SELECT
	// var tbl_select, tbl_where tblaccount
	// DB_InitTable(&tbl_select, &tbl_where)

	// tbl_select.PlayerKey = "Yea"
	// tbl_select.GameDBID = 9999
	// tbl_select.ConnectTime = time.Now()

	// tbl_where.ConnectIP = "127.0.0.1"
	// // tbl_where.GameDBID = 1
	// // tbl_where.UserUUID = 10

	// tbl_arr, err := DB_SELECT(db, tbl_select, tbl_where) //, "UserUUID = 10")
	// for _, d := range tbl_arr {
	// 	fmt.Println(d)
	// }

	// INSERT
	// var tbl_in1, tbl_in2 tblaccount
	// DB_InitTable(&tbl_in1, &tbl_in2)

	// tbl_in1.PlayerKey = "hello1"
	// tbl_in1.UserUUID = 20
	// tbl_in1.ConnectIP = "127.0.0.1"
	// tbl_in1.ConnectTime = time.Now().UTC()
	// tbl_in1.CreateTime = time.Now().UTC()
	// tbl_in1.GameDBID = 21

	// tbl_in2.PlayerKey = "hello2"
	// tbl_in2.UserUUID = 30
	// tbl_in2.ConnectIP = "127.0.0.1"
	// tbl_in2.ConnectTime = time.Now().UTC()
	// tbl_in2.CreateTime = time.Now().UTC()
	// tbl_in2.GameDBID = 31

	// aff, err := DB_INSERT(db, tbl_in1, tbl_in2)
	// fmt.Println(aff)
}
