package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

var createTab = `CREATE TABLE %s.%s on cluster monitor(date Date DEFAULT toDate(0), 
				%s, 
				val Float64, 
				ts DateTime,
				updated DateTime DEFAULT now())
				ENGINE=MergeTree(date, (%s, ts), 8192)`
var createView = `CREATE TABLE %s.%s_view on cluster monitor(date Date DEFAULT toDate(0),
				%s, 
				val Float64, 
				ts DateTime,
				updated DateTime DEFAULT now())
				ENGINE=Distributed(monitor, %s, %s, rand())`

var tableSet = map[string]string{}

func (w *p2cWriter) Create(req *p2cRequest) error{
	_, ok := tableSet[req.name]
	if ok {
		return nil
	}

	// ensure tags are inserted in the same order each time
	// possibly/probably impacts indexing?
	sort.Strings(req.tags)
	var columns bytes.Buffer
	var keys bytes.Buffer
	for i, tag := range req.tags {
		kv := strings.Split(tag, "=")
		if strings.EqualFold("__name__", kv[0]) {
			continue
		}

		if i == len(req.tags)-1 {
			columns.WriteString("ch_" + kv[0] + " String")
			keys.WriteString("ch_" + kv[0])
		} else {
			columns.WriteString("ch_" + kv[0] + " String, ")
			keys.WriteString("ch_" + kv[0] + ",")
		}

	}
	createTabSQL := fmt.Sprintf(createTab, w.conf.ChDB, req.name, columns.String(), keys.String())
	tx, err := w.db.Begin()
	if err != nil {
		w.logger.With(writerContent...).Errorf("begin transaction: %s", err.Error())
		w.ko.Add(1.0)
		return err
	}
	err = w.createTable(tx, createTabSQL)
	if err != nil {
		tx.Rollback()
		w.logger.With(writerContent...).Errorf("SQL : %s", createTabSQL)
		return err
	}
	tx.Commit()

	createViewSQL := fmt.Sprintf(createView, w.conf.ChDB, req.name, columns.String(), w.conf.ChDB, req.name)
	tx, err = w.db.Begin()
	if err != nil {
		w.logger.With(writerContent...).Errorf("begin transaction: %s", err.Error())
		w.ko.Add(1.0)
		return err
	}
	err = w.createTable(tx, createViewSQL)
	if err != nil {
		tx.Rollback()
		w.logger.With(writerContent...).Errorf("SQL : %s", createViewSQL)
		return err
	}
	tx.Commit()
	tableSet[req.name] = ""
	return nil
}

//Please see https://github.com/kshvakov/clickhouse/issues/33
func (w *p2cWriter) createTable(tx *sql.Tx, sql string) error {
	smt, err := tx.Prepare(sql)
	defer smt.Close()
	if err != nil {
		w.logger.With(writerContent...).Errorf("prepare statement: %s", err.Error())
		w.ko.Add(1.0)
		return err
	}
	_, err = smt.Exec()
	if err != nil {
		w.logger.With(writerContent...).Errorf("create table failed: %s", err.Error())
		w.ko.Add(1.0)
		return err
	}
	return nil
}

var insertSQL = `INSERT INTO %s.%s_view
	(date, %s, val, ts)
	VALUES	(?, %s, ?, ?)`
func (w *p2cWriter) Insert(smt *sql.Stmt, req *p2cRequest) error {

	// ensure tags are inserted in the same order each time
	// possibly/probably impacts indexing?
	sort.Strings(req.tags)

	var values []interface{}
	values = append(values, req.ts)
	for i, tag := range req.tags {
		kv := strings.Split(tag, "=")
		if strings.EqualFold("__name__", kv[0]) {
			continue
		}
		if i == len(req.tags)-1 {
			values = append(values, kv[1])
		} else {
			values = append(values, kv[1])
		}
	}

	values = append(values, req.val)
	values = append(values, req.ts)
	_, err := smt.Exec(values...)
	if err != nil {
		w.logger.With(writerContent...).Errorf("statement exec: %s", err.Error())
		w.logger.With(writerContent...).Errorf("Req: %s", req)
		w.ko.Add(1.0)
		return err
	}
	return nil
}

func (w *p2cWriter) BuildInsertSql(req *p2cRequest) string{
	// ensure tags are inserted in the same order each time
	// possibly/probably impacts indexing?
	sort.Strings(req.tags)
	var keys bytes.Buffer
	var replaceHolder bytes.Buffer
	for i, tag := range req.tags {
		kv := strings.Split(tag, "=")
		if strings.EqualFold("__name__", kv[0]) {
			continue
		}
		if i == len(req.tags)-1 {
			keys.WriteString("ch_" + kv[0])
			replaceHolder.WriteString("?")
		} else {
			keys.WriteString("ch_" + kv[0] + ", ")
			replaceHolder.WriteString("?, ")
		}
	}
	return fmt.Sprintf(insertSQL, w.conf.ChDB, req.name, keys.String(), replaceHolder.String())
}

func (w *p2cWriter) LoadTable() error {
	loadSql := fmt.Sprintf("SHOW TABLES FROM %s", w.conf.ChDB)
	// post them to db all at once
	tx, err := w.db.Begin()
	if err != nil {
		w.logger.With(writerContent...).Errorf("prepare statement: %s", err.Error())
		return err
	}

	smt, err := tx.Prepare(loadSql)
	if err != nil {
		w.logger.With(writerContent...).Errorf("prepare statement: %s", err.Error())
		return err
	}
	defer smt.Close()

	tableR, err := smt.Query()
	if err != nil {
		w.logger.With(writerContent...).Errorf("prepare statement: %s", err.Error())
		return err
	}
	defer tableR.Close()

	var table string
	for tableR.Next() {
		err = tableR.Scan(&table)
		if err != nil {
			w.logger.With(writerContent...).Errorf("prepare statement: %s", err.Error())
			return err
		}
		//columns, err := w.loadColumn(tx, table)
		//if err != nil {
		//	return err
		//}
		//
		//var replaceHolder []string
		//for range columns {
		//	replaceHolder = append(replaceHolder, "?")
		//}
		//tableSet[table] = fmt.Sprintf("INSERT INTO %s.%s(%s)values(%s)", w.conf.ChDB, table,
		//	strings.Join(columns, ","), strings.Join(replaceHolder, ","))
		tableSet[table] = ""
	}
	return nil
}

func (w *p2cWriter) loadColumn(tx *sql.Tx, table string) ([]string, error) {

	loadSql := fmt.Sprintf("DESC %s.%s", w.conf.ChDB, table)
	smt, err := tx.Prepare(loadSql)
	if err != nil {
		return nil, err
	}
	defer smt.Close()

	columnR, err := smt.Query()
	if err != nil {
		return nil, err
	}
	defer columnR.Close()

	var columns []string
	var column string
	for columnR.Next() {
		columnR.Scan(&column)
		columns = append(columns, column)
	}

	return columns, nil
}

func (w *p2cWriter) hasTable(table string) bool {
	_, ok := tableSet[table]
	return ok
}
