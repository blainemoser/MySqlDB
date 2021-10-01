package database

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blainemoser/TrySql/trysql"
)

var ts *trysql.TrySql

var testDatabase string

const letterBytes = "abcyuizxvd"

var tdb *Database = nil

var resultCode int

const userTable = `CREATE TABLE users (
	id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(1000) NOT NULL,
	email VARCHAR(1000) NOT NULL,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)`

const widgetTable = `CREATE TABLE widgets (
	id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	sku VARCHAR(1000) NOT NULL,
	description VARCHAR (2500),
	weight FLOAT,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)`

const seedWidgets = `INSERT INTO widgets (sku, description, weight) VALUES (?,?,?), (?,?,?), (?,?,?)`

func TestMain(m *testing.M) {
	defer tearDown()
	var err error
	ts, err = trysql.Initialise([]string{"-v", "latest"})
	if err != nil {
		panic(err)
	}
	err = bootstrap()
	if err != nil {
		panic(err)
	}
	resultCode = m.Run()
}

func tearDown() {
	if tdb != nil {
		tdb.Close()
	}
	td := func(r interface{}) {
		if ts != nil {
			err := ts.TearDown()
			if err != nil {
				log.Println(err.Error())
			}
		}
		if r == nil {
			os.Exit(resultCode)
		}
		panic(r)
	}
	r := recover()
	if r != nil {
		td(r)
	}
	td(nil)
}

func recovery(t *testing.T) {
	r := recover()
	if r != nil {
		t.Error(r)
	}
}

func TestMakeSchemalessAndSetSchema(t *testing.T) {
	defer recovery(t)
	checkSetSchema(t)
	checkHasSchema(t, tdb, testDatabase)
}

func TestMake(t *testing.T) {
	defer recovery(t)
	configs := getConfigs(true)
	testName := tdb.Name() + "_new_test_schema"
	configs.Database = testName
	newDatabase, err := Make(configs)
	if err != nil {
		t.Error(err)
	}
	if newDatabase.Name() != testName {
		t.Errorf("expected database name to be %s, got %s", testName, newDatabase.Name())
	}
	newDatabase.Close()
}

func TestExec(t *testing.T) {
	defer recovery(t)
	_, err := tdb.Exec(userTable, nil)
	if err != nil {
		t.Error(err)
	}
	err = checkHasTable("users")
	if err != nil {
		t.Error(err)
	}
}

func TestConfigSupplementing(t *testing.T) {
	newDB := testDatabase + "_test_supplementation"
	_, err := tdb.Exec(fmt.Sprintf("create schema %s", newDB), nil)
	setEnvVars()
	if err != nil {
		t.Error(err)
	}
	configs := &Configs{
		Database: newDB,
	}
	d, err := Make(configs)
	if err != nil {
		t.Error(err)
	}
	defer d.Close()
	// This should have created a database instance by supplementing the configs with the set env vars
	checkHasSchema(t, tdb, newDB)
}

func TestQueryRaw(t *testing.T) {
	defer recovery(t)
	createWidgetsTable(t)
	rows, err := tdb.QueryRaw("select sku from widgets where id > ? order by created_at desc limit 1", []interface{}{
		1,
	})
	if err != nil {
		t.Error(err)
	}
	var errs []string
	for _, row := range rows {
		if row["sku"] == nil {
			errs = append(errs, "sku not found in row")
		}
		_, ok := row["sku"].(string)
		if !ok {
			errs = append(errs, "expected sku to be a string")
		}
	}
	err = collateErrors(errs)
	if err != nil {
		t.Error(err)
	}
}

func TestRow(t *testing.T) {
	defer recovery(t)
	createWidgetsTable(t)
	row, err := tdb.Row("select sku, description from widgets where id = ?", 2)
	if err != nil {
		t.Error(err)
	}
	var errs []string
	for col, mapped := range row {
		mappedStr, ok := mapped.(string)
		if !ok {
			errs = append(errs, fmt.Sprintf("expected %s to be a string", col))
		}
		if col == "description" {
			if mappedStr != "Widget Two" {
				errs = append(errs, fmt.Sprintf("expected 'description' to be 'Widget Two', got %s", mappedStr))
			}
		}
		if col == "sku" {
			if mappedStr != "WIDG2" {
				errs = append(errs, fmt.Sprintf("expected 'sku' to be 'WIDG2', got %s", mappedStr))
			}
		}
	}
	err = collateErrors(errs)
	if err != nil {
		t.Error(err)
	}
}

func TestRowByStringField(t *testing.T) {
	defer recovery(t)
	createWidgetsTable(t)
	row, err := tdb.RowByStringField("select sku, description, weight from widgets where description = ?", "Widget Three")
	if err != nil {
		t.Error(err)
	}
	var errs []string
	for col, mapped := range row {
		if mappedStr, ok := mapped.(string); ok {
			if col == "description" {
				if mappedStr != "Widget Three" {
					errs = append(errs, fmt.Sprintf("expected 'description' to be 'Widget Three', got %s", mappedStr))
				}
			}
			if col == "sku" {
				if mappedStr != "WIDG3" {
					errs = append(errs, fmt.Sprintf("expected 'sku' to be 'WIDG3', got %s", mappedStr))
				}
			}
		} else if mappedFloat, ok := mapped.(float64); ok {
			if mappedFloat != 1.23 {
				errs = append(errs, fmt.Sprintf("expected 'weight' to be '%f', got %f", 1.23, mappedFloat))
			}
		} else {
			errs = append(errs, fmt.Sprintf("expected %s to be either a string or float 64", col))
		}
	}
	err = collateErrors(errs)
	if err != nil {
		t.Error(err)
	}
}

func TestCreateRecord(t *testing.T) {
	defer recovery(t)
	createWidgetsTable(t)
	_, err := tdb.MakeRecord(map[string]interface{}{
		"sku":         "WIDG4",
		"description": "Widget Four",
		"weight":      100.9,
	}, "widgets").Create()
	if err != nil {
		t.Error(err)
	}
	checkWidgetExists(t, "WIDG4")
}

func TestUpdateRecord(t *testing.T) {
	defer recovery(t)
	createWidgetsTable(t)
	_, err := tdb.MakeRecord(map[string]interface{}{
		"weight": 110.9,
		"sku":    "WIDG4",
	}, "widgets").Update("sku")
	if err != nil {
		t.Error(err)
	}
	checkWidgetUpdated(t, "WIDG4")
}

func bootstrap() error {
	configs := getConfigs(true)
	d, err := MakeSchemaless(configs)
	if err != nil {
		return err
	}
	err = createSchema(&d)
	if err != nil {
		return err
	}

	return nil
}

func setDatabase() {
	now := time.Now().Unix()
	by := strconv.FormatInt(now, 10)
	result := make([]byte, len(by))
	for i := 0; i < len(by); i++ {
		place, err := strconv.Atoi(string(by[i]))
		if err != nil {
			place = 0
		}
		result[i] = letterBytes[place]
	}
	testDatabase = string(result)
}

func getConfigs(schemaless bool) *Configs {
	var db string
	if schemaless {
		db = ""
	} else {
		db = testDatabase
	}
	return &Configs{
		Port:     ts.HostPortStr(),
		Host:     "127.0.0.1",
		Username: "root",
		Password: ts.Password(),
		Database: db,
		Driver:   "mysql",
	}
}

func createSchema(d *Database) error {
	if len(testDatabase) < 1 {
		setDatabase()
	}
	_, err := d.Exec(fmt.Sprintf("create schema %s", testDatabase), nil)
	if err != nil {
		return err
	}
	d.Close()
	d.SetSchema(testDatabase)
	tdb = d
	return nil
}

func checkSetSchema(t *testing.T) {
	if tdb.Name() != testDatabase {
		t.Errorf("expected schema name to be %s, got %s", testDatabase, tdb.Name())
	}
}

func checkHasSchema(t *testing.T, d *Database, db string) {
	schemas, err := d.QueryRaw(
		fmt.Sprintf("select schema_name from information_schema.schemata where schema_name = '%s'", db),
		nil,
	)
	if err != nil {
		t.Error(err)
	}
	if len(schemas) < 1 {
		t.Errorf("expected database %s to exist, not found", db)
	}
	checkSchemaExists(t, schemas, db)
}

func checkSchemaExists(t *testing.T, schemas []map[string]interface{}, db string) {
	hasResult := false
	for _, v := range schemas {
		if v["SCHEMA_NAME"] == nil {
			continue
		}
		if name, ok := v["SCHEMA_NAME"].(string); ok {
			if name == db {
				hasResult = true
				break
			}
		}
	}

	if !hasResult {
		t.Errorf("expected database %s to exist, not found", db)
	}
}

func checkHasTable(table string) error {
	tables, err := tdb.QueryRaw("SHOW TABLES", nil)
	if err != nil {
		return err
	}
	if len(tables) < 1 {
		return fmt.Errorf("no tables found")
	}
	key := fmt.Sprintf("Tables_in_%s", tdb.Name())
	exists := false
	for _, v := range tables {
		if v[key] == nil {
			continue
		}
		if v[key] == table {
			exists = true
			break
		}
	}
	if !exists {
		return fmt.Errorf("could not find table '%s'", table)
	}
	return nil
}

func createWidgetsTable(t *testing.T) {
	err := checkHasTable("widgets")
	if err != nil {
		if err.Error() == fmt.Sprintf("could not find table '%s'", "widgets") {
			_, err = tdb.Exec(widgetTable, nil)
			if err != nil {
				t.Error(err)
			}
			err = seedWidgetsTable()
			if err != nil {
				t.Error(err)
			}
			return
		}
		t.Error(err)
	}
}

func seedWidgetsTable() error {
	_, err := tdb.Exec(
		seedWidgets,
		[]interface{}{
			"WIDG1",
			"Widget One",
			12.3,
			"WIDG2",
			"Widget Two",
			34.5,
			"WIDG3",
			"Widget Three",
			1.23,
		},
	)
	if err != nil {
		return err
	}
	return nil
}

func checkRows(t *testing.T, rows *sql.Rows) {
	mappedRows, err := parseRowResults(rows)
	if err != nil {
		t.Error(err)
	}
	err = checkMappedRows(mappedRows)
	if err != nil {
		t.Error(err)
	}
}

func checkMappedRows(mappedRows []map[string]interface{}) error {
	var errs []string
	for _, row := range mappedRows {
		if row["sku"] == nil {
			errs = append(errs, "no sku found in row")
			continue
		}
		if row["description"] == nil {
			errs = append(errs, "no description found in row")
			continue
		}
		_, ok := row["description"].(string)
		if !ok {
			errs = append(errs, "expected type of description to be string")
		}
		_, ok = row["weight"].(float64)
		if !ok {
			errs = append(errs, "expected type of weight to be a float 64")
		}
	}
	return collateErrors(errs)
}

func collateErrors(errs []string) error {
	if len(errs) < 1 {
		return nil
	}
	return fmt.Errorf(strings.Join(errs, ", "))
}

func setEnvVars() {
	os.Setenv("DB_PORT", ts.HostPortStr())
	os.Setenv("DB_HOST", "127.0.0.1")
	os.Setenv("DB_USERNAME", "root")
	os.Setenv("DB_PASSWORD", ts.Password())
	os.Setenv("DB_CONNECTION", "mysql")
}

func checkWidgetExists(t *testing.T, widg string) {
	_, err := tdb.RowByStringField("select * from widgets where sku = ?", widg)
	if err != nil {
		t.Error(err)
	}
}

func checkWidgetUpdated(t *testing.T, widg string) {
	widget, err := tdb.RowByStringField("select * from widgets where sku = ?", widg)
	if err != nil {
		t.Error(err)
	}
	if widget["weight"] == nil {
		t.Errorf("weight not found")
	}
	weight, ok := widget["weight"].(float64)
	if !ok {
		t.Errorf("expected weight to be a float 64")
	}
	if math.Abs(weight-110.9) > 0.1 {
		t.Errorf("expected weight to be %f, got %f", 110.9, weight)
	}
}
