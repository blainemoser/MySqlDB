package database

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"

	_ "github.com/go-sql-driver/mysql"
)

// Database is a database connection
type Database struct {
	connection *sql.DB
	configs    *Configs
	Schemaless bool
}

type Configs struct {
	Host     string
	Username string
	Password string
	Port     string
	Database string
	Driver   string
}

// Make creates a new Database instance
func Make(configs *Configs) (Database, error) {
	database := Database{
		connection: nil,
		configs:    configs,
		Schemaless: false,
	}

	database.setConfigs(false)
	database.connect()
	return database, nil
}

// MakeSchemaless makes a Database instance without a schema yet
func MakeSchemaless(configs *Configs) (Database, error) {

	database := Database{
		connection: nil,
		configs:    configs,
		Schemaless: true,
	}

	database.setConfigs(true)
	database.connect()
	return database, nil
}

// Close closes the database instance's connection
func (database *Database) Close() {
	database.connection.Close()
}

// Exec executes a query statement
func (d *Database) Exec(query string, inserts []interface{}) (sql.Result, error) {
	if inserts != nil {
		return d.connection.Exec(query, inserts[:]...)
	}
	return d.connection.Exec(query)
}

// Name returns the name of the database instance
func (database *Database) Name() string {
	return database.configs.Database
}

func (d *Database) setConfigs(schemaless bool) {
	// Check whether the configs need to be supplemented with Environment Vars
	if d.hasAllConfigs(schemaless) {
		return
	}

	d.supplementConfigs(schemaless)
}

func (d *Database) hasAllConfigs(schemaless bool) bool {
	var hasDB bool
	if schemaless {
		hasDB = true
	} else {
		hasDB = len(d.configs.Database) > 0
	}
	return hasDB &&
		len(d.configs.Driver) > 0 &&
		len(d.configs.Host) > 0 &&
		len(d.configs.Password) > 0 &&
		len(d.configs.Port) > 0 &&
		len(d.configs.Username) > 0
}

func (d *Database) connect() {
	// connect to database
	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%s)/",
		d.configs.Username,
		d.configs.Password,
		d.configs.Host,
		d.configs.Port,
	)
	if !d.Schemaless {
		connectionString += d.configs.Database
	}
	connection, err := sql.Open(d.configs.Driver, connectionString)
	d.connection = connection
	if err != nil {
		log.Fatal(err)
	}
}

// SetSchema sets a DB instance to having a schema
func (d *Database) SetSchema(schemaName string) {
	d.configs.Database = schemaName
	d.Schemaless = false
	d.connect()
}

// QueryRaw runs a raw select query against the database
func (d *Database) QueryRaw(query string, escaped []interface{}) ([]map[string]interface{}, error) {
	rowResult, err := d.getRowResult(query, escaped)
	if err != nil {
		return nil, err
	}
	return parseRowResults(rowResult)
}

func parseRowResults(rowResult *sql.Rows) ([]map[string]interface{}, error) {
	cols, err := rowResult.Columns()
	if err != nil {
		return nil, err
	}
	typeMapping, err := getTypeMapping(rowResult)
	if err != nil {
		return nil, err
	}

	return rowResultWalk(rowResult, cols, typeMapping)
}

func getTypeMapping(rowResult *sql.Rows) (map[string]string, error) {
	colTypes, err := rowResult.ColumnTypes()
	if err != nil {
		return nil, err
	}
	typeMapping := mapColTypes(colTypes)
	return typeMapping, nil
}

func getResultantRow(cols []string, typeMapping map[string]string, rowResult *sql.Rows) (map[string]interface{}, error) {
	row := makeRow(typeMapping, cols)
	err := rowResult.Scan(row...)
	if err != nil {
		return nil, err
	}
	resultRow := make(map[string]interface{})
	var count = 0
	for _, v := range row {
		rowValue := getRowValue(v)
		resultRow[cols[count]] = rowValue
		count++
	}
	return resultRow, nil
}

func rowResultWalk(rowResult *sql.Rows, cols []string, typeMapping map[string]string) ([]map[string]interface{}, error) {
	defer rowResult.Close()
	result := make([]map[string]interface{}, 0)
	for rowResult.Next() {
		resultRow, err := getResultantRow(cols, typeMapping, rowResult)
		if err != nil {
			return nil, err
		}
		result = append(result, resultRow)
	}
	err := rowResult.Err()
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Rows gets rows from the query
func (d *Database) Rows(query string, escaped []interface{}) (*sql.Rows, error) {
	return d.getRowResult(query, escaped)
}

func (d *Database) getRowResult(query string, escaped []interface{}) (*sql.Rows, error) {
	rows, err := d.getRows(query, escaped)
	if err != nil {
		return nil, err
	}
	if !reflect.ValueOf(rows).CanInterface() {
		return nil, errors.New("Rows not found")
	}
	rowResult, ok := (reflect.ValueOf(rows).Interface()).(*sql.Rows)
	if !ok {
		return nil, errors.New("type-assertion error on rows")
	}
	return rowResult, nil
}

// Row gets a row from the query
func (d *Database) Row(query string, id int64) *sql.Row {
	row := d.connection.QueryRow(query, id)
	return row
}

// Row gets a row from the query
func (d *Database) RowByStringField(query string, field string) *sql.Row {
	row := d.connection.QueryRow(query, field)
	return row
}

func (d *Database) getRows(query string, escaped []interface{}) (interface{}, error) {
	if escaped != nil {
		rows, err := d.connection.Query(query, escaped[:]...)
		if err != nil {
			return nil, err
		}

		return rows, nil
	} else {
		rows, err := d.connection.Query(query)
		if err != nil {
			return nil, err
		}

		return rows, nil
	}
}

// maps column types
func mapColTypes(colTypes []*sql.ColumnType) map[string]string {
	typeMapping := make(map[string]string)

	for _, v := range colTypes {
		typeMapping[v.Name()] = v.DatabaseTypeName()
	}

	return typeMapping
}

// gets the value of a row
func getRowValue(row interface{}) interface{} {
	rowValueOf := reflect.ValueOf(row)
	if rowValueOf.Kind() == reflect.Interface || rowValueOf.Kind() == reflect.Ptr {
		rowValue := rowValueOf.Elem()
		if rowValue.CanInterface() {
			rowValue := rowValue.Interface()

			// TODO add all sql.* types
			if stringVal, ok := (rowValue).(sql.NullString); ok {
				return stringVal.String
			}

			if intVal, ok := (rowValue).(sql.NullInt64); ok {
				return intVal.Int64
			}

			if floatVal, ok := (rowValue).(sql.NullFloat64); ok {
				return floatVal.Float64
			}

			return rowValue
		}
		return rowValue
	}
	return row
}

// makes a new row based on the database column type returned
func makeRow(typeMapping map[string]string, cols []string) []interface{} {
	row := make([]interface{}, 0)
	for _, v := range cols {
		switch typeMapping[v] {
		case "INT":
			var newCol sql.NullInt64
			row = append(row, &newCol)
		case "BIT":
			var newCol sql.NullInt64
			row = append(row, &newCol)
		case "TINYINT":
			var newCol sql.NullInt64
			row = append(row, &newCol)
		case "BOOL":
			var newCol sql.NullInt64
			row = append(row, &newCol)
		case "BOOLEAN":
			var newCol sql.NullInt64
			row = append(row, &newCol)
		case "SMALLINT":
			var newCol sql.NullInt64
			row = append(row, &newCol)
		case "MEDIUMINT":
			var newCol sql.NullInt64
			row = append(row, &newCol)
		case "INTEGER":
			var newCol sql.NullInt64
			row = append(row, &newCol)
		case "BIGINT":
			var newCol sql.NullInt64
			row = append(row, &newCol)
		case "FLOAT":
			var newCol sql.NullFloat64
			row = append(row, &newCol)
		case "DOUBLE":
			var newCol sql.NullFloat64
			row = append(row, &newCol)
		case "DECIMAL":
			var newCol sql.NullFloat64
			row = append(row, &newCol)
		case "DEC":
			var newCol sql.NullFloat64
			row = append(row, &newCol)
		case "CHAR":
			var newCol sql.NullString
			row = append(row, &newCol)
		case "VARCHAR":
			var newCol sql.NullString
			row = append(row, &newCol)
		case "BINARY":
			var newCol sql.NullString
			row = append(row, &newCol)
		case "VARBINARY":
			var newCol sql.NullString
			row = append(row, &newCol)
		case "TINYBLOB":
			var newCol sql.NullString
			row = append(row, &newCol)
		case "TINYTEXT":
			var newCol sql.NullString
			row = append(row, &newCol)
		case "TEXT":
			var newCol sql.NullString
			row = append(row, &newCol)
		case "BLOB":
			var newCol sql.NullString
			row = append(row, &newCol)
		case "MEDIUMTEXT":
			var newCol sql.NullString
			row = append(row, &newCol)
		case "MEDIUMBLOB":
			var newCol sql.NullString
			row = append(row, &newCol)
		case "LONGTEXT":
			var newCol sql.NullString
			row = append(row, &newCol)
		case "LONGBLOB":
			var newCol sql.NullString
			row = append(row, &newCol)
		case "ENUM":
			var newCol sql.NullString
			row = append(row, &newCol)
		case "SET":
			var newCol sql.NullString
			row = append(row, &newCol)
		case "DATE":
			var newCol sql.NullString
			row = append(row, &newCol)
		case "DATETIME":
			var newCol sql.NullString
			row = append(row, &newCol)
		case "TIMESTAMP":
			var newCol sql.NullString
			row = append(row, &newCol)
		case "TIME":
			var newCol sql.NullString
			row = append(row, &newCol)
		case "YEAR":
			var newCol sql.NullString
			row = append(row, &newCol)
		default:
			var newCol sql.NullString
			row = append(row, &newCol)
		}
	}

	return row
}

func (d *Database) supplementConfigs(schemaless bool) {
	envVars := envConfigs()
	for key, value := range envVars {
		if key == "database" && schemaless {
			continue
		}
		d.setDBConfig(key, value)
	}
}

func (d *Database) setDBConfig(key, value string) {
	switch key {
	case "host":
		if len(d.configs.Host) < 1 {
			d.configs.Host = value
		}
	case "username":
		if len(d.configs.Username) < 1 {
			d.configs.Username = value
		}
	case "password":
		if len(d.configs.Password) < 1 {
			d.configs.Password = value
		}
	case "port":
		if len(d.configs.Port) < 1 {
			d.configs.Port = value
		}
	case "database":
		if len(d.configs.Database) < 1 {
			d.configs.Database = value
		}
	case "driver":
		if len(d.configs.Driver) < 1 {
			d.configs.Driver = value
		}
	default:
		break
	}
}

func envConfigs() map[string]string {
	return getEnvVars(map[string]string{
		"host":     "DB_HOST",
		"username": "DB_USERNAME",
		"password": "DB_PASSWORD",
		"port":     "DB_PORT",
		"database": "DB_DATABASE",
		"driver":   "DB_CONNECTION",
	})
}

func getEnvVars(input map[string]string) map[string]string {
	result := make(map[string]string)
	for inputName, varName := range input {
		result[inputName] = os.Getenv(varName)
	}

	return result
}
