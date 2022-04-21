package main

import (
	"database/sql"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type tblaccount struct {
	PlayerKey   string `PK:"true"`
	UserUUID    int64
	ConnectIP   string
	ConnectTime time.Time
	CreateTime  time.Time
	GameDBID    int
	SnsID       string
	PlatformIdx int
}

const (
	SQL_SELECT = iota + 100
	SQL_INSERT
	SQL_UPDATE
	SQL_UPSERT
	SQL_DELETE
	SQL_INCRESE
	SQL_DECRESE
	SQL_INSERT_SELECT
	SQL_UPDATE_SELECT
	SQL_UPSERT_SELECT

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

			case reflect.TypeOf(time.Time{}).Kind():
				v.Set(reflect.ValueOf(time.Time{}))
			}
		}
	}
}

func DB_NewTable[TableType interface{}](tbl TableType, count int) []TableType {
	var ret []TableType

	for i := 0; i < count; i++ {
		var obj TableType
		DB_InitTable(&obj)
		ret = append(ret, obj)
	}

	return ret
}

func DB_Make_SELECT_Query(tbl_columns interface{}, tbl_where interface{}, raw_condition ...string) (string, error) {

	/*
		Extract the columns that will be affected from the SELECT UPDATE INSERT syntax.
		SELECT UPDATE INSERT 구문에서 영향 받을 컬럼들부터 추출.
	*/
	var target_column []string

	tbl_val := reflect.ValueOf(tbl_columns)
	tbl_type := reflect.TypeOf(tbl_columns)
	from_table := reflect.TypeOf(tbl_columns).Name()

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
	var where_column []string
	var where_index []int
	var where_val []reflect.Value

	tbl_where_val := reflect.ValueOf(tbl_where)
	tbl_where_type := reflect.TypeOf(tbl_where)

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
	if 0 != len(queryWhereElems) {
		where_str = (" WHERE " + strings.Join(queryWhereElems, " AND "))
	}

	if 0 < len(raw_condition) {
		where_str += (" " + raw_condition[0])
	}

	queryStr := "SELECT " + strings.Join(target_column, ", ") + " FROM " + from_table + where_str + ";"
	fmt.Println(queryStr)

	return queryStr, nil
}

func DB_Make_INSERT_Query[DB_Table interface{}](tbl_insert ...DB_Table) (string, error) {

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
		queryStr += (strings.Join(tbl_elem_array, ", ") + ";")
		fmt.Println(queryStr)
	}

	return queryStr, nil
}

func DB_Make_UPDATE_Query(tbl_columns interface{}, tbl_where interface{}, raw_condition ...string) (string, error) {

	from_table := reflect.TypeOf(tbl_columns).Name()
	queryStr := "UPDATE " + from_table + " SET "

	{
		var set_field_cmd []string
		tbl_val := reflect.ValueOf(tbl_columns)
		tbl_type := reflect.TypeOf(tbl_columns)
		for i := 0; i < tbl_val.NumField(); i++ {
			reflect_v := tbl_val.Field(i)
			reflect_t := tbl_type.Field(i).Name
			if true == DB_IsUse(reflect_v) {
				set_field_cmd = append(set_field_cmd, reflect_t+"="+DB_ToString(reflect_v))
			}
		}

		queryStr += strings.Join(set_field_cmd, ", ")
	}

	{
		var where_str string
		var set_field_cmd []string
		tbl_val := reflect.ValueOf(tbl_where)
		tbl_type := reflect.TypeOf(tbl_where)
		for i := 0; i < tbl_val.NumField(); i++ {
			reflect_v := tbl_val.Field(i)
			reflect_t := tbl_type.Field(i).Name
			if true == DB_IsUse(reflect_v) {
				set_field_cmd = append(set_field_cmd, reflect_t+"="+DB_ToString(reflect_v))
			}
		}

		if 0 != len(set_field_cmd) {
			where_str = " WHERE " + strings.Join(set_field_cmd, " AND ")
		}

		if 0 < len(raw_condition) {
			where_str += (" " + raw_condition[0])
		}

		queryStr += (where_str + ";")
	}

	fmt.Println(queryStr)
	return queryStr, nil
}

func DB_Make_DELETE_Query(tbl_where interface{}, raw_condition ...string) (string, error) {
	from_table := reflect.TypeOf(tbl_where).Name()
	queryStr := "DELETE FROM " + from_table

	{
		var where_str string
		var set_field_cmd []string
		tbl_val := reflect.ValueOf(tbl_where)
		tbl_type := reflect.TypeOf(tbl_where)
		for i := 0; i < tbl_val.NumField(); i++ {
			reflect_v := tbl_val.Field(i)
			reflect_t := tbl_type.Field(i).Name
			if true == DB_IsUse(reflect_v) {
				set_field_cmd = append(set_field_cmd, reflect_t+"="+DB_ToString(reflect_v))
			}
		}

		if 0 != len(set_field_cmd) {
			where_str = " WHERE " + strings.Join(set_field_cmd, " AND ")
		}

		if 0 < len(raw_condition) {
			where_str += (" " + raw_condition[0])
		}

		queryStr += (where_str + ";")
	}

	fmt.Println(queryStr)
	return queryStr, nil
}

func DB_Make_UPSERT_Query[DB_Table interface{}](tbl_insert DB_Table) (string, error) {

	queryStr := "INSERT INTO "

	/*
		This function optionally allows you to attempt to INSERT only a few columns.
		이 함수에서는 선택적으로 특정 몇몇 컬럼만을 INSERT 하려고 하는 행위를 허용함.
	*/
	var valid_field_index []int

	var not_pk_col_index []int
	var field_names []string
	var elem_row_value []string
	{
		/*
			Extract the table name and each column name.
			테이블 명과 각 컬럼명을 추출.
		*/
		tbl := reflect.ValueOf(&tbl_insert).Elem()
		field_size := tbl.NumField()
		tbl_type := reflect.TypeOf(&tbl_insert).Elem()

		for i := 0; i < field_size; i++ {
			t := tbl_type.Field(i)

			if true == DB_IsUse(tbl.Field(i)) {
				field_names = append(field_names, t.Name)
				valid_field_index = append(valid_field_index, i)
				_, thisIsPK := t.Tag.Lookup("PK")
				if false == thisIsPK {
					not_pk_col_index = append(not_pk_col_index, i)
				}
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
		tbl := reflect.ValueOf(&tbl_insert).Elem()
		field_size := tbl.NumField()

		// elem_row_value == arr[ field1_value, field2_value, ... ]
		for i := 0; i < field_size; i++ {
			val := tbl.Field(i)

			if true == DB_IsUse(val) {
				elem_row_value = append(elem_row_value, DB_ToString(val))
			}
		}

		// elem_row_value make row like strings => (field1_value, field2_value, ...)
		tbl_elem_array = append(tbl_elem_array, "("+strings.Join(elem_row_value, ", ")+")")

		// queryStr's final query form => INSERT INTO tbl (col1, col2, ...) VALUES (val1, val2, ...) ON DUPLICATE KEY UPDATE
		queryStr += (strings.Join(tbl_elem_array, ", ") + " ON DUPLICATE KEY UPDATE ")
	}
	{
		/*
			UPDATE 구문 추가
		*/
		var name_val_set_query_elems []string
		for _, i := range not_pk_col_index {
			name_val_set_query_elems = append(name_val_set_query_elems, field_names[i]+"="+elem_row_value[i])
		}

		queryStr += (strings.Join(name_val_set_query_elems, ", ") + "; ")
		fmt.Println(queryStr)
	}

	return queryStr, nil
}

func DB_Make_INCR_Query(tbl_columns interface{}, tbl_where interface{}, size int64, raw_condition ...string) (string, error) {

	from_table := reflect.TypeOf(tbl_columns).Name()
	queryStr := "UPDATE " + from_table + " SET "

	{
		var set_field_cmd []string
		tbl_val := reflect.ValueOf(tbl_columns)
		tbl_type := reflect.TypeOf(tbl_columns)
		for i := 0; i < tbl_val.NumField(); i++ {
			reflect_v := tbl_val.Field(i)
			reflect_t := tbl_type.Field(i).Name
			if true == DB_IsUse(reflect_v) {
				var operator string
				var s int64
				if 0 <= size {
					operator = "+"
					s = size
				} else {
					operator = "-"
					s = size * -1
				}
				set_field_cmd = append(set_field_cmd, reflect_t+"="+reflect_t+operator+strconv.FormatInt(s, 10))
			}
		}

		queryStr += strings.Join(set_field_cmd, ", ")
	}

	{
		var where_str string
		var set_field_cmd []string
		tbl_val := reflect.ValueOf(tbl_where)
		tbl_type := reflect.TypeOf(tbl_where)
		for i := 0; i < tbl_val.NumField(); i++ {
			reflect_v := tbl_val.Field(i)
			reflect_t := tbl_type.Field(i).Name
			if true == DB_IsUse(reflect_v) {
				set_field_cmd = append(set_field_cmd, reflect_t+"="+DB_ToString(reflect_v))
			}
		}

		if 0 != len(set_field_cmd) {
			where_str = " WHERE " + strings.Join(set_field_cmd, " AND ")
		}

		if 0 < len(raw_condition) {
			where_str += (" " + raw_condition[0])
		}

		queryStr += (where_str + ";")
	}

	fmt.Println(queryStr)
	return queryStr, nil
}

/*
	< How To Use >
	ex)
		type tblaccount struct {
			PlayerKey   string `PK:"true"`
			UserUUID    int64
			ConnectIP   string
			ConnectTime time.Time
			CreateTime  time.Time
			GameDBID    int
			SnsID       string
			PlatformIdx int
		}

		var tbl_select, tbl_where tblaccount
		DB_InitTable(&tbl_select, &tbl_where)	<- Init member Values

		tbl_select.PlayerKey = "Yea"			<- It doesn't matter what value you set
		tbl_select.GameDBID = 9999				<- It doesn't matter what value you set

		tbl_where.ConnectIP = "127.0.0.1"		<- Setting value is important in setting query conditions
		tbl_where.GameDBID = 1					<- Setting value is important in setting query conditions
		tbl_where.UserUUID = 10					<- Setting value is important in setting query conditions

		DB_SELECT(db, &tbl_select, &tbl_where)	<- Sended Query : SELECT PlayerKey, GameDBID FROM tblaccount WHERE UserUUID = 10 AND ConnectIP = "127.0.0.1" AND GameDBID = 1

		DB_SELECT(db, tbl_select, tblaccount{}, "ORDER BY UserUUID ASC Limit 10") <- Sended Query : SELECT PlayerKey, GameDBID FROM tblaccount ORDER BY UserUUID ASC Limit 10
*/
func DB_SELECT[DB_Table interface{}](db *sql.DB, tbl_target DB_Table, tbl_where DB_Table, raw_condition ...string) ([]DB_Table, error) {

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

	queryStr, err := DB_Make_SELECT_Query(tbl_target, tbl_where, raw_condition...)
	if err != nil {
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
			return retValues, err
		}

		retValues = append(retValues, obj)
	}

	return retValues, nil
}

/*
	< How To Use >
	ex)
		type tblaccount struct {
			PlayerKey   string `PK:"true"`
			UserUUID    int64
			ConnectIP   string
			ConnectTime time.Time
			CreateTime  time.Time
			GameDBID    int
			SnsID       string
			PlatformIdx int
		}

		var tbl_in1, tbl_in2 tblaccount
		DB_InitTable(&tbl_in1, &tbl_in2)

		tbl_in1.PlayerKey = "hello1"
		tbl_in1.UserUUID = 20
		tbl_in1.ConnectIP = "127.0.0.1"
		tbl_in1.ConnectTime = time.Now().UTC()
		tbl_in1.CreateTime = time.Now().UTC()
		tbl_in1.GameDBID = 21
		tbl_in1.SnsID = "helloSNSid"
		tbl_in1.PlatformIdx = 2

		tbl_in2.PlayerKey = "hello2"
		tbl_in2.UserUUID = 30
		tbl_in2.ConnectIP = "127.0.0.1"
		tbl_in2.ConnectTime = time.Now().UTC()
		tbl_in2.CreateTime = time.Now().UTC()
		tbl_in2.GameDBID = 31
		tbl_in1.SnsID = "helloSNSid"
		tbl_in1.PlatformIdx = 2

		// INSERT INTO tblaccount (PlayerKey, UserUUID, ConnectIP, ConnectTime, CreateTime, GameDBID, SnsID, PlatformIdx)
		// VALUES
		// ("hello1", 20, "127.0.0.1", "2022-04-14 02:16:00", "2022-04-14 02:16:00", 21, "helloSNSid", 2),
		// ("hello2", 30, "127.0.0.1", "2022-04-14 02:16:00", "2022-04-14 02:16:00", 31, "helloSNSid", 2);
		aff, err := DB_INSERT(db, tbl_in1, tbl_in2)		<- Multiple tables can be treated as a single query.
*/
func DB_INSERT[DB_Table interface{}](db *sql.DB, tbl_insert ...DB_Table) (int64, error) {

	if 1 > len(tbl_insert) {
		return 0, errors.New(fmt.Sprint("[ SQL ERROR ] There is no data for INSERT"))
	}

	/*
		Error handling if any of the table column values to be INSERT are abnormal.
		INSERT 할 테이블 컬럼 값이 하나라도 비정상인 경우 에러처리.
	*/
	{
		/*
			If a code error occurs at the time of INSERT due to the auto-increase column, this part can be commented.
			자동 증가 컬럼 때문에 INSERT 하는 시점에 코드 에러가 발생한다면, 이 부분을 주석처리하면 된다.
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
	}

	queryStr, err := DB_Make_INSERT_Query(tbl_insert...)
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

/*
	< How To Use >
	ex)
		type tblaccount struct {
			PlayerKey   string `PK:"true"`
			UserUUID    int64
			ConnectIP   string
			ConnectTime time.Time
			CreateTime  time.Time
			GameDBID    int
			SnsID       string
			PlatformIdx int
		}

		tbl_arr := DB_NewTable(tblaccount{}, 2)				<- Multiple initialization tables can be created and received in an array type.
		tbl_arr[0].UserUUID = 100							<- Enter the value you want to UPDATE.
		tbl_arr[1].PlayerKey = "hello1"						<- Enter a value in WHERE to be a conditional reference.

		// UPDATE tblaccount SET UserUUID=100 WHERE PlayerKey="hello1";
		aff, err := DB_UPDATE(db, tbl_arr[0], tbl_arr[1])	<- Enter DB connection objects, tables to update, and where table in order.
*/
func DB_UPDATE[DB_Table interface{}](db *sql.DB, tbl_target DB_Table, tbl_where DB_Table, raw_condition ...string) (int64, error) {

	/*
		Check that each table type is the same.
		각 테이블 타입이 동일한지 체크.
	*/
	{
		tb_col_t := reflect.TypeOf(&tbl_target)
		tb_where_t := reflect.TypeOf(&tbl_where)

		if tb_col_t.Name() != tb_where_t.Name() {
			return 0, errors.New(fmt.Sprint("[ SQL ERROR ] SQL Table Not Same -", tb_col_t.Elem().Name(), ":", tb_where_t.Elem().Name()))
		}
	}

	queryStr, err := DB_Make_UPDATE_Query(tbl_target, tbl_where, raw_condition...)
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

/*
	< How To Use >
	ex)
		type tblaccount struct {
			PlayerKey   string `PK:"true"`
			UserUUID    int64
			ConnectIP   string
			ConnectTime time.Time
			CreateTime  time.Time
			GameDBID    int
			SnsID       string
			PlatformIdx int
		}

		tbl_del := DB_NewTable(tblaccount{}, 1)[0]
		tbl_del.PlayerKey = "hello2"

		// DELETE tblaccount WHERE PlayerKey="hello2"
		aff, err := DB_DELETE(db, tbl_del)
*/
func DB_DELETE[DB_Table interface{}](db *sql.DB, tbl_where DB_Table, raw_condition ...string) (int64, error) {

	queryStr, err := DB_Make_DELETE_Query(tbl_where, raw_condition...)
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

/*
	< How To Use >
	ex)
		type tblaccount struct {
			PlayerKey   string `PK:"true"`		<- For UPSERT commands, PK tag values must be entered.
			UserUUID    int64
			ConnectIP   string
			ConnectTime time.Time
			CreateTime  time.Time
			GameDBID    int
			SnsID       string
			PlatformIdx int
		}

		var tbl_upsert tblaccount
		tbl_upsert.PlayerKey = "hi"
		tbl_upsert.UserUUID = 28
		tbl_upsert.ConnectIP = "127.0.0.1"
		tbl_upsert.ConnectTime = time.Now().UTC()
		tbl_upsert.CreateTime = time.Now().UTC()
		tbl_upsert.GameDBID = 19
		tbl_upsert.SnsID = "hihi"
		tbl_upsert.PlatformIdx = 97

		// INSERT INTO tblaccount (PlayerKey, UserUUID, ConnectIP, ConnectTime, CreateTime, GameDBID, SnsID, PlatformIdx)
		// VALUES ("hi", 28, "127.0.0.1", "2022-04-14 02:16:00", "2022-04-14 02:16:00", 19, "hihi", 97)
		// ON DUPLICATE KEY
		// UPDATE UserUUID=28, ConnectIP="127.0.0.1", ConnectTime="2022-04-14 02:16:00", CreateTime="2022-04-14 02:16:00", GameDBID=97, SnsID="hihi", PlatformIdx=97;
		aff, err := DB_UPSERT(db, tbl_upsert)	<- At the time of writing the UPSERT query command, check the PK tag and write the UPDATE syntax except for the corresponding column.
*/
func DB_UPSERT[DB_Table interface{}](db *sql.DB, tbl_upsert DB_Table) (int64, error) {

	queryStr, err := DB_Make_UPSERT_Query(tbl_upsert)
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

/*
	< How To Use >
	ex)
		type tblaccount struct {
			PlayerKey   string `PK:"true"`
			UserUUID    int64
			ConnectIP   string
			ConnectTime time.Time
			CreateTime  time.Time
			GameDBID    int
			SnsID       string
			PlatformIdx int
		}

		var tbl_target, tbl_where tblaccount
		DB_InitTable(&tbl_target, &tbl_where)
		tbl_target.UserUUID = 0			<- It doesn't matter what value you set
		tbl_target.GameDBID = 0			<- It doesn't matter what value you set

		tbl_where.PlayerKey = "hi"		<- Setting value is important in setting query conditions

		// UPDATE tblaccount SET UserUUID=UserUUID+1000, GameDBID=GameDBID+1000 WHERE PlayerKey="hi";
		aff, err := DB_INCR(db, tbl_target, tbl_where, 1000)
*/
func DB_INCR[DB_Table interface{}](db *sql.DB, tbl_target DB_Table, tbl_where DB_Table, size int64, raw_condition ...string) (int64, error) {

	/*
		Check that each table type is the same.
		각 테이블 타입이 동일한지 체크.
	*/
	{
		tb_col_t := reflect.TypeOf(&tbl_target)
		tb_where_t := reflect.TypeOf(&tbl_where)

		if tb_col_t.Name() != tb_where_t.Name() {
			return 0, errors.New(fmt.Sprint("[ SQL ERROR ] SQL Table Not Same -", tb_col_t.Elem().Name(), ":", tb_where_t.Elem().Name()))
		}
	}

	queryStr, err := DB_Make_INCR_Query(tbl_target, tbl_where, size, raw_condition...)
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

/*
	< How To Use >
	ex)
		type tblaccount struct {
			PlayerKey   string `PK:"true"`
			UserUUID    int64
			ConnectIP   string
			ConnectTime time.Time
			CreateTime  time.Time
			GameDBID    int
			SnsID       string
			PlatformIdx int
		}

		var tbl_target, tbl_where tblaccount
		DB_InitTable(&tbl_target, &tbl_where)
		tbl_target.UserUUID = 0			<- It doesn't matter what value you set
		tbl_target.GameDBID = 0			<- It doesn't matter what value you set

		tbl_where.PlayerKey = "hi"		<- Setting value is important in setting query conditions

		// UPDATE tblaccount SET UserUUID=UserUUID-1000, GameDBID=GameDBID-1000 WHERE PlayerKey="hi";
		aff, err := DB_DECR(db, tbl_target, tbl_where, 1000)
*/
func DB_DECR[DB_Table interface{}](db *sql.DB, tbl_target DB_Table, tbl_where DB_Table, size int64, raw_condition ...string) (int64, error) {
	return DB_INCR(db, tbl_target, tbl_where, size*-1, raw_condition...)
}

/*
	< How To Use >
	ex)
		type tblaccount struct {
			PlayerKey   string `PK:"true"`
			UserUUID    int64
			ConnectIP   string
			ConnectTime time.Time
			CreateTime  time.Time
			GameDBID    int
			SnsID       string
			PlatformIdx int
		}

		var tbl_in1, tbl_sel tblaccount
		DB_InitTable(&tbl_in1, &tbl_sel)

		tbl_in1.PlayerKey = "hello1"
		tbl_in1.UserUUID = 20
		tbl_in1.ConnectIP = "127.0.0.1"
		tbl_in1.ConnectTime = time.Now().UTC()
		tbl_in1.CreateTime = time.Now().UTC()
		tbl_in1.GameDBID = 21
		tbl_in1.SnsID = "wow1"
		tbl_in1.PlatformIdx = 99

		tbl_sel.PlayerKey = ""					<- It doesn't matter what value you set
		tbl_sel.UserUUID = -210					<- It doesn't matter what value you set
		tbl_sel.ConnectIP = "127.0.0.1"			<- It doesn't matter what value you set

		// After executing the INSERT syntax, a query request is made to select only the required columns.
		arr_list, err := DB_INSERT_SELECT(db, tbl_in1, tbl_sel)
*/
func DB_INSERT_SELECT[DB_Table interface{}](db *sql.DB, tbl_insert DB_Table, tbl_select DB_Table, raw_condition ...string) ([]DB_Table, error) {

	/*
		In the case of an auto-increment column, since it may be an empty column, we do not check that all columns have values.
		DB에서 자동 증가 컬럼으로 정의되어있는 경우, INSERT 시점에 비어서 넣어야 하는 컬럼인 경우가 있으므로, 여기선 모든 컬럼에 값이 있는지 체크하지 않는다.
	*/

	var retValues []DB_Table
	queryStr, err := DB_Make_INSERT_Query(tbl_insert)
	if err != nil {
		return retValues, err
	}

	res, err := db.Exec(queryStr)
	if err != nil {
		return retValues, err
	}

	_, err = res.RowsAffected()
	if err != nil {
		fmt.Println("[ SQL ERROR ] Rows Affected error - ", err)
		return retValues, err
	}

	queryStr, err = DB_Make_SELECT_Query(tbl_select, tbl_insert, raw_condition...)
	if err != nil {
		return retValues, err
	}

	/*
		Save the source member variable index value to receive the value.
		값을 받을 원본 멤버 변수 인덱스 값을 저장.
	*/
	var target_index []int
	tbl_val := reflect.ValueOf(&tbl_select).Elem()
	for i := 0; i < tbl_val.NumField(); i++ {
		if true == DB_IsUse(tbl_val.Field(i)) {
			target_index = append(target_index, i)
		}
	}

	rows, err := db.Query(queryStr)
	if err != nil {
		return retValues, err
	}

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
			return retValues, err
		}

		retValues = append(retValues, obj)
	}

	return retValues, nil
}

/*
	< How To Use >
	ex)
		type tblaccount struct {
			PlayerKey   string `PK:"true"`
			UserUUID    int64
			ConnectIP   string
			ConnectTime time.Time
			CreateTime  time.Time
			GameDBID    int
			SnsID       string
			PlatformIdx int
		}

		tbl_arr := DB_NewTable(tblaccount{}, 2)
		tbl_arr[0].UserUUID = 100
		tbl_arr[1].PlayerKey = "hello1"

		// Set UserUUID with PlayerKey "hello1" to 100 and select all columns with PlayerKey "hello1"
		arr_list, err := DB_UPDATE_SELECT(db, tbl_arr[0], tbl_arr[1], tblaccount{})
*/
func DB_UPDATE_SELECT[DB_Table interface{}](db *sql.DB, tbl_target DB_Table, tbl_where DB_Table, tbl_select DB_Table, raw_condition ...string) ([]DB_Table, error) {

	var retValues []DB_Table
	_, err := DB_UPDATE(db, tbl_target, tbl_where)
	if err != nil {
		return retValues, err
	}

	return DB_SELECT(db, tbl_select, tbl_where, raw_condition...)
}

func DB_UPSERT_SELECT[DB_Table interface{}](db *sql.DB, tbl_upsert DB_Table, tbl_select DB_Table, raw_condition ...string) ([]DB_Table, error) {

	var retValues []DB_Table
	_, err := DB_UPSERT(db, tbl_upsert)
	if err != nil {
		return retValues, err
	}

	return DB_SELECT(db, tbl_select, tbl_upsert, raw_condition...)
}

func DB_INCR_SELECT[DB_Table interface{}](db *sql.DB, tbl_target DB_Table, tbl_where DB_Table, size int64, raw_condition ...string) ([]DB_Table, error) {

	var retValues []DB_Table
	_, err := DB_INCR(db, tbl_target, tbl_where, size)
	if err != nil {
		return retValues, err
	}

	return DB_SELECT(db, tbl_target, tbl_where, raw_condition...)
}

func DB_DECR_SELECT[DB_Table interface{}](db *sql.DB, tbl_target DB_Table, tbl_where DB_Table, size int64, raw_condition ...string) ([]DB_Table, error) {

	var retValues []DB_Table
	_, err := DB_DECR(db, tbl_target, tbl_where, size)
	if err != nil {
		return retValues, err
	}

	return DB_SELECT(db, tbl_target, tbl_where, raw_condition...)
}

type DBJob struct {
	queryList  []string
	jobCounter int
	errorMap   map[int]error
}

/*
	SQL_INSERT, SQL_UPDATE, SQL_UPSERT, SQL_DELETE, SQL_INCRESE, SQL_DECRESE

	Transaction is supported only for the above functions, and DBJob function is not supported for other commands.
	위 함수들에 대해서만 Transaction 을 지원하며, 다른 명령에 대해서는 DBJob 기능을 지원하지 않음.
*/
func AddJob[DB_Table interface{}](dbjob *DBJob, command int, tables ...DB_Table) error {
	var err error = nil

	switch command {
	case SQL_INSERT:

		if 1 > len(tables) {
			err = errors.New("[ DBJob Error ] AddJob - no Job added")
			break
		}

		tbl_name := reflect.TypeOf(tables[0]).Name()

		for _, tbl_in := range tables {
			elemTbl_name := reflect.TypeOf(tbl_in).Name()
			if tbl_name != elemTbl_name {
				err = errors.New("[ DBJob Error ] AddJob - Insert Job's element table name is not same")
				break
			}
		}

		str, err := DB_Make_INSERT_Query(tables...)
		if err != nil {
			break
		}

		dbjob.queryList = append(dbjob.queryList, str)
	case SQL_UPDATE:

		if 2 > len(tables) {
			err = errors.New("[ DBJob Error ] AddJob - no Job added")
			break
		}

		str, err := DB_Make_UPDATE_Query(tables[0], tables[1])
		if err != nil {
			break
		}

		dbjob.queryList = append(dbjob.queryList, str)
	case SQL_UPSERT:

		if 1 > len(tables) {
			err = errors.New("[ DBJob Error ] AddJob - no Job added")
			break
		}

		str, err := DB_Make_UPSERT_Query(tables[0])
		if err != nil {
			break
		}

		dbjob.queryList = append(dbjob.queryList, str)
	case SQL_DELETE:

		if 1 > len(tables) {
			err = errors.New("[ DBJob Error ] AddJob - no Job added")
			break
		}

		str, err := DB_Make_DELETE_Query(tables[0])
		if err != nil {
			break
		}

		dbjob.queryList = append(dbjob.queryList, str)
	case SQL_INCRESE:

		if 2 > len(tables) {
			err = errors.New("[ DBJob Error ] AddJob - no Job added")
			break
		}

		str, err := DB_Make_INCR_Query(tables[0], tables[1], 1)
		if err != nil {
			break
		}

		dbjob.queryList = append(dbjob.queryList, str)
	case SQL_DECRESE:

		if 2 > len(tables) {
			err = errors.New("[ DBJob Error ] AddJob - no Job added")
			break
		}

		str, err := DB_Make_INCR_Query(tables[0], tables[1], -1)
		if err != nil {
			break
		}

		dbjob.queryList = append(dbjob.queryList, str)

	default:
		err = errors.New("[ DBJob Error ] AddJob - Invalid command")
	}

	dbjob.jobCounter += 1
	if err != nil {
		dbjob.errorMap[dbjob.jobCounter] = err
	}

	return err
}

/*
	When the actual query is executed is when the DBJob.Run() function is called.
	실제 쿼리가 실행되는 시점은 DBJob.Run() 함수를 호출하는 시점
*/
func (dbjob *DBJob) Run(db *sql.DB) (int64, error) {
	var err error = nil
	if 0 != len(dbjob.errorMap) {
		for k, v := range dbjob.errorMap {
			fmt.Println("[ DBJob Error ] Run Failed - AddJob was failed. ::: No.", k, " - ", v)
		}
		return 0, errors.New("[ DBJob Error ] Run Failed.")
	}

	if 1 > len(dbjob.queryList) {
		return 0, errors.New("[ DBJob Error ] Run Failed. No Jobs")
	}

	var tx *sql.Tx = nil
	var res sql.Result
	var affCount int64 = 0

	if 1 < dbjob.jobCounter {
		tx, err = db.Begin()
	}

	for i, query := range dbjob.queryList {
		res, err = db.Exec(query)
		if err != nil {
			fmt.Println("[ DBJob ERROR ] Job index : ", i, " - ", err)
			if tx != nil {
				tx.Rollback()
			}
			return 0, err
		}
		affect, err := res.RowsAffected()
		if err != nil {
			fmt.Println("[ DBJob ERROR ] Job index : ", i, " - Rows Affected error - ", err)
		}
		affCount += affect
	}

	if tx != nil {
		tx.Commit()
	}

	return affCount, err
}

func main() {

	db, err := sql.Open("mysql", "connection ip")
	if err != nil {
		fmt.Println(err)
	}
	defer db.Close()

	// SELECT ---------------------------------------------------------------------
	// var tbl_select, tbl_where tblaccount
	// DB_InitTable(&tbl_select, &tbl_where)

	// tbl_select.PlayerKey = ""
	// tbl_select.UserUUID = 0
	// tbl_select.GameDBID = 0
	// tbl_select.ConnectTime = time.Now()

	// tbl_where.ConnectIP = "127.0.0.1"
	// // tbl_where.GameDBID = 1
	// // tbl_where.UserUUID = 10

	// tbl_arr, err := DB_SELECT(db, tbl_select, tbl_where, "ORDER BY UserUUID DESC limit 2")
	// for _, d := range tbl_arr {
	// 	fmt.Println(d)
	// }

	// INSERT ---------------------------------------------------------------------
	// var tbl_in1, tbl_in2 tblaccount
	// DB_InitTable(&tbl_in1, &tbl_in2)

	// tbl_in1.PlayerKey = "hello1"
	// tbl_in1.UserUUID = 20
	// tbl_in1.ConnectIP = "127.0.0.1"
	// tbl_in1.ConnectTime = time.Now().UTC()
	// tbl_in1.CreateTime = time.Now().UTC()
	// tbl_in1.GameDBID = 21
	// tbl_in1.SnsID = "helloSNSid"
	// tbl_in1.PlatformIdx = 2

	// tbl_in2.PlayerKey = "hello2"
	// tbl_in2.UserUUID = 30
	// tbl_in2.ConnectIP = "127.0.0.1"
	// tbl_in2.ConnectTime = time.Now().UTC()
	// tbl_in2.CreateTime = time.Now().UTC()
	// tbl_in2.GameDBID = 31
	// tbl_in1.SnsID = "helloSNSid"
	// tbl_in1.PlatformIdx = 2

	// aff, err := DB_INSERT(db, tbl_in1, tbl_in2)
	// fmt.Println(aff)

	// UPDATE ---------------------------------------------------------------------
	// tbl_arr := DB_NewTable(tblaccount{}, 2)
	// tbl_arr[0].UserUUID = 100
	// tbl_arr[1].PlayerKey = "hello1"

	// aff, err := DB_UPDATE(db, tbl_arr[0], tbl_arr[1])
	// fmt.Println(aff)

	// DELETE ---------------------------------------------------------------------
	// tbl_del := DB_NewTable(tblaccount{}, 1)[0]
	// tbl_del.PlayerKey = "hello2"
	// aff, err := DB_DELETE(db, tbl_del)
	// fmt.Println(aff)

	// UPSERT ---------------------------------------------------------------------
	// var tbl_upsert tblaccount
	// tbl_upsert.PlayerKey = "hi"
	// tbl_upsert.UserUUID = 28
	// tbl_upsert.ConnectIP = "127.0.0.1"
	// tbl_upsert.ConnectTime = time.Now().UTC()
	// tbl_upsert.CreateTime = time.Now().UTC()
	// tbl_upsert.SnsID = "hihi"
	// tbl_upsert.GameDBID = 19

	// aff, err := DB_UPSERT(db, tbl_upsert)
	// fmt.Println(aff)

	// INCRESE ---------------------------------------------------------------------
	// var tbl_target, tbl_where tblaccount
	// DB_InitTable(&tbl_target, &tbl_where)
	// tbl_target.UserUUID = 0
	// tbl_target.GameDBID = 0
	// tbl_where.PlayerKey = "hi"

	// aff, err := DB_DECR(db, tbl_target, tbl_where, 1000)
	// fmt.Println(aff)

	// INSERT SELECT ---------------------------------------------------------------
	// var tbl_in1, tbl_sel tblaccount
	// DB_InitTable(&tbl_in1, &tbl_sel)

	// tbl_in1.PlayerKey = "hello1"
	// tbl_in1.UserUUID = 20
	// tbl_in1.ConnectIP = "127.0.0.1"
	// tbl_in1.ConnectTime = time.Now().UTC()
	// tbl_in1.CreateTime = time.Now().UTC()
	// tbl_in1.GameDBID = 21
	// tbl_in1.SnsID = "wow1"
	// tbl_in1.PlatformIdx = 99

	// tbl_sel.PlayerKey = "hello2"
	// tbl_sel.UserUUID = 30
	// tbl_sel.ConnectIP = "127.0.0.1"

	// arr_list, err := DB_INSERT_SELECT(db, tbl_in1, tbl_sel)
	// fmt.Println(arr_list)

	// UPDATE SELECT ---------------------------------------------------------------
	// tbl_arr := DB_NewTable(tblaccount{}, 2)
	// tbl_arr[0].UserUUID = 100
	// tbl_arr[1].PlayerKey = "hello1"

	// arr_list, err := DB_UPDATE_SELECT(db, tbl_arr[0], tbl_arr[1], tblaccount{})
	// fmt.Println(arr_list)

	// UPSERT SELECT ---------------------------------------------------------------
	// var tbl_upsert tblaccount
	// tbl_upsert.PlayerKey = "hello1"
	// tbl_upsert.UserUUID = 310
	// tbl_upsert.ConnectIP = "127.0.0.1"
	// tbl_upsert.ConnectTime = time.Now().UTC()
	// tbl_upsert.CreateTime = time.Now().UTC()
	// tbl_upsert.GameDBID = 19
	// tbl_upsert.SnsID = "wow113"
	// tbl_upsert.PlatformIdx = 99

	// arr_list, err := DB_UPSERT_SELECT(db, tbl_upsert, tbl_upsert)
	// fmt.Println(arr_list)

	// DBJOB ---------------------------------------------------------------
	// var dbjob DBJob
	// {
	// 	var tbl_insert tblaccount
	// 	tbl_insert.PlayerKey = "dbjob_test"
	// 	tbl_insert.UserUUID = 900
	// 	tbl_insert.ConnectIP = "127.0.0.1"
	// 	tbl_insert.ConnectTime = time.Now().UTC()
	// 	tbl_insert.CreateTime = time.Now().UTC()
	// 	tbl_insert.GameDBID = 899
	// 	tbl_insert.SnsID = "dbjob"
	// 	tbl_insert.PlatformIdx = 99

	// 	AddJob(&dbjob, SQL_INSERT, tbl_insert)

	// 	var tbl_where tblaccount
	// 	DB_InitTable(&tbl_insert, &tbl_where)
	// 	tbl_insert.UserUUID = 953
	// 	tbl_where.PlayerKey = "dbjob_test"
	// 	AddJob(&dbjob, SQL_UPDATE, tbl_insert, tbl_where)

	// 	tbl_insert.PlayerKey = "dbjob_test"
	// 	tbl_insert.UserUUID = 333
	// 	tbl_insert.ConnectIP = "127.0.0.1"
	// 	tbl_insert.ConnectTime = time.Now().UTC()
	// 	tbl_insert.CreateTime = time.Now().UTC()
	// 	tbl_insert.GameDBID = 777
	// 	tbl_insert.SnsID = "dbjob"
	// 	tbl_insert.PlatformIdx = 97
	// 	AddJob(&dbjob, SQL_UPSERT, tbl_insert)

	// 	DB_InitTable(&tbl_insert)
	// 	tbl_insert.PlatformIdx = 0
	// 	AddJob(&dbjob, SQL_INCRESE, tbl_insert, tbl_where)
	// 	AddJob(&dbjob, SQL_INCRESE, tbl_insert, tbl_where)
	// }
	// arr, err := dbjob.Run(db)
	// fmt.Println(arr, err)
}
