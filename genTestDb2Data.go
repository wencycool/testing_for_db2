package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	_ "github.com/ibmdb/go_ibm_db"
	"log"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

var Ids int64
var db *sql.DB
var err error

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	_ = os.Setenv("DB2HOME", "$DB2_HOME")
	_ = os.Setenv("CGO_CFLAGS", "-I$DB2HOME/include")
	_ = os.Setenv("CGO_LDFLAGS", "-L$DB2HOME/lib ")
	_ = os.Setenv("LD_LIBRARY_PATH", "$DB2HOME/lib")
	//db.SetMaxOpenConns(500)
	//db.SetConnMaxLifetime(time.Second * 100)
	//db.SetMaxIdleConns(40)

}

/*
export DB2HOME=$DB2_HOME
export CGO_CFLAGS=-I$DB2HOME/include
export CGO_LDFLAGS=-L$DB2HOME/lib
export LD_LIBRARY_PATH=$DB2_HOME/lib
*/

func main() {
	var (
		num_q                    int
		host                     string
		port                     int
		user                     string
		password                 string
		dbname                   string
		tabname                  string
		tbspace                  string
		colcount                 int
		pk                       bool
		rowsOneCommit            int
		table                    *Table
		maxLoggerTimeMillseconds int
		delta_S                  int //打印日志时间间隔
	)

	//初始化连接
	//num_q = 200
	flag.StringVar(&host, "host", "localhost", "IP")
	flag.IntVar(&port, "port", 60000, "port")
	flag.StringVar(&dbname, "dbname", "sample", "database name")
	flag.StringVar(&user, "user", "db2inst1", "user name")
	flag.StringVar(&password, "pwd", "db2inst1", "password")
	flag.StringVar(&tabname, "tabname", "ttt", "the table will be created")
	flag.StringVar(&tbspace, "tbspace", "", "tablespace name")
	flag.BoolVar(&pk, "pk", true, "set default primary key")
	flag.IntVar(&rowsOneCommit, "rowsOneCommit", 1, " how many rows insert per commit")
	flag.IntVar(&num_q, "parallel", 100, "parallel connections")
	flag.IntVar(&colcount, "colcount", 100, "how many columns will be generated")
	flag.IntVar(&maxLoggerTimeMillseconds, "logtime", 10, "超过多少毫秒则被日志记录下来")
	flag.IntVar(&delta_S, "delta", 1, "打印日志时间间隔")
	flag.Parse()
	var logger = new(log.Logger)
	logger.SetOutput(os.Stdout)
	logger.SetFlags(log.LstdFlags)
	var db *sql.DB
	var err error
	conn := fmt.Sprintf("HOSTNAME=%s;PORT=%d;PROTOCOL=TCPIP;DATABASE=%s;UID=%s;PWD=%s", host, port, dbname, user, password)
	if db, err = sql.Open("go_ibm_db", conn); err != nil {
		log.Fatal(err)
	}
	c, _ := db.Conn(context.Background())
	table = GenTable(tabname, tbspace, colcount, pk)
	//检查表是否存在,如果存在则不创建表，直接插入数据
	if t, err := FindTable(c, table); err == nil && t == nil {
		log.Printf("Table:%s not exists,create it\n", table.Tabname)
		//创建测试表
		if _, err := db.Exec(table.CreateTableSQL()); err != nil {
			log.Fatal(err)
		}

	} else if err == nil && t != nil {
		log.Printf("Table:%s already exist,just insert data \n", table.Tabname)
		table = t
		if err := InitIds(c, table, &Ids); err != nil {
			log.Fatal(err)
		}
	} else if err != nil {
		log.Fatal(err)
	}
	stats := new(InsertStats)
	q := make(chan *sql.Conn, num_q)
	for i := 0; i < num_q; i++ {
		c, _ := db.Conn(context.Background())
		q <- c
	}
	for i := 0; i < num_q; i++ {
		go func() {
			for {
				conn := <-q
				InsertData(conn, table, rowsOneCommit, &Ids, maxLoggerTimeMillseconds, stats)
				//执行完毕后把连接扔回管道中
				q <- conn
			}

		}()
	}
	go func() {
		tk := time.NewTicker(time.Duration(delta_S) * time.Second)
		for {
			select {
			case <-tk.C:
				//开始打印统计数据
				logger.Printf("Insert次数:%-8d,Insert超时次数:%-8d,Insert平均执行时间:%-8d,Insert超时平均执行时间:%-8d,"+
					"Commit次数:%-8d,Commit超时次数:%-8d,Commit平均执行时间:%-8d,Commit超时平均执行时间:%-8d\n",
					atomic.LoadInt64(&stats.TotalInserts), atomic.LoadInt64(&stats.TotalInsertOverflows), atomic.LoadInt64(&stats.TotalInsertTime)/(1+atomic.LoadInt64(&stats.TotalInserts)), atomic.LoadInt64(&stats.TotalInsertOverflowTime)/(1+atomic.LoadInt64(&stats.TotalInsertOverflows)),
					atomic.LoadInt64(&stats.TotalCommits), atomic.LoadInt64(&stats.TotalCommitOverflows), atomic.LoadInt64(&stats.TotalCommitTime)/(1+atomic.LoadInt64(&stats.TotalCommits)), atomic.LoadInt64(&stats.TotalCommitOverflowTIme)/(1+atomic.LoadInt64(&stats.TotalCommitOverflows)),
				)
			}
		}
	}()
	select {}
}

//全局唯一记录生成器
func InitIds(conn *sql.Conn, tabname *Table, ids_ptr *int64) error {
	var (
		colname string
	)
	for _, col := range tabname.Cols {
		if col.ISPK || col.Type == BIGINT || col.Type == INT {
			colname = col.COLNAME
		}
	}
	if table, err := FindTable(conn, tabname); err != nil {
		return err
	} else if table == nil {
		return errors.New("Table not exists")
	} else {
		sqltext := fmt.Sprintf("select max(%s) from %s", colname, tabname.Tabname)
		row := conn.QueryRowContext(context.Background(), sqltext)
		if err := row.Scan(ids_ptr); err != nil {
			return errors.New(err.Error() + " sql:" + sqltext)
		} else {
			return nil
		}
	}

}

//生成随机字符串
func GetRandomString(l int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyz"
	bytes := []byte(str)
	result := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < l; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return string(result)
}

//ids_ptr 是唯一值来源,logtime超过多少毫秒则被日志记录
func InsertData(conn *sql.Conn, table *Table, rowsOneCommit int, ids_ptr *int64, logtime int, stats *InsertStats) {
	//总执行时间，总执行次数，语句总执行时间，语句总执行次数，提交总执行次数，提交总执行时间
	var (
		totalActTime             int
		totalActExec             int
		totalActExecOverflows    int
		totalCommitTime          int
		totalCommitExec          int
		totalCommitExecOverflows int
		totalActOverflowTime     int
		TotalCommitOverflowTIme  int
	)
	rand.Seed(time.Now().Unix())
	f := func(conn *sql.Conn, table *Table, ids_ptr *int64) (string, []interface{}) {
		cols := make([]string, 0)
		colParm := make([]string, 0)
		values := make([]interface{}, 0)
		for _, col := range table.Cols {
			cols = append(cols, col.COLNAME)
			colParm = append(colParm, "?")
			switch col.Type {
			case INT, BIGINT:
				if col.ISPK {
					values = append(values, int(atomic.AddInt64(ids_ptr, 1)))
				} else {
					values = append(values, rand.Intn(1000000))
				}
			case VARCHAR, CHAR:
				values = append(values, GetRandomString(col.LENGTH))
			}
		}
		colStr := strings.Join(cols, ",")
		colParmStr := strings.Join(colParm, ",")
		//cols := strings.Repeat("?,", len(table.Cols))
		//cols := cols[0:len(cols) - 1]
		sql := fmt.Sprintf("insert into %s (%s) values (%s)", table.Tabname, colStr, colParmStr)
		return sql, values
	}
	for {
		var (
			LastRows = rowsOneCommit
		)
		tx, _ := conn.BeginTx(context.Background(), nil)
		for LastRows > 0 {
			sql, values := f(conn, table, ids_ptr)
			btime := time.Now()
			if r, err := tx.Exec(sql, values...); err != nil {
				log.Println(err)
				return
			} else {
				if rowseffect, err := r.RowsAffected(); err != nil {
					log.Println(err)
					break
				} else {
					atime := time.Now()
					timeDiff := atime.Sub(btime)
					totalActExec++
					totalActTime += int(timeDiff.Milliseconds())
					atomic.AddInt64(&stats.TotalInserts, int64(totalActExec))
					atomic.AddInt64(&stats.TotalInsertTime, int64(totalActTime))
					if timeDiff > time.Duration(int64(logtime))*time.Millisecond {
						totalActExecOverflows++
						totalActOverflowTime += int(timeDiff.Milliseconds())
						atomic.AddInt64(&stats.TotalInsertOverflows, int64(totalActExecOverflows))
						atomic.AddInt64(&stats.TotalInsertOverflowTime, int64(totalActOverflowTime))
						//logger.Printf("Insert,Time:%-5d,TotalActExec:%-5d,TotalActTime:%-5d,AvgActTime:%-5d,Overflows:%-5d\n",timeDiff.Milliseconds(),totalActExec,totalActTime.Milliseconds(),int(totalActTime.Milliseconds())/totalActExec,totalActExecOverflows)
					}
					LastRows = LastRows - int(rowseffect)
				}
			}

		}
		//记录commit时间
		btime := time.Now()
		tx.Commit()
		atime := time.Now()
		timeDiff := atime.Sub(btime)
		totalCommitExec++
		totalCommitTime += int(timeDiff.Milliseconds())
		atomic.AddInt64(&stats.TotalCommits, int64(totalCommitExec))
		atomic.AddInt64(&stats.TotalCommitTime, int64(totalCommitTime))
		if timeDiff > time.Duration(int64(logtime))*time.Millisecond {
			totalCommitExecOverflows++
			TotalCommitOverflowTIme += int(timeDiff.Milliseconds())
			atomic.AddInt64(&stats.TotalCommitOverflows, int64(totalCommitExecOverflows))
			atomic.AddInt64(&stats.TotalCommitOverflowTIme, int64(TotalCommitOverflowTIme))
			//logger.Println("COMMIT,Time:",timeDiff.Milliseconds())
			//logger.Printf("Commit,Time:%-5d,TotalCommitExec:%-5d,TotalCommitTime:%-5d,AvgCommitTime:%-5d,Overflows:%-5d\n",timeDiff.Milliseconds(),totalCommitExec,totalCommitTime.Milliseconds(),int(totalCommitTime.Milliseconds())/totalCommitExec,totalCommitExecOverflows)
		}

	}
	return
}

type InsertStats struct {
	TotalInserts            int64 //总插入次数
	TotalCommits            int64 //总提交次数
	TotalInsertTime         int64 //总插入时间 millseconds
	TotalCommitTime         int64 //总提交时间 millseconds
	TotalInsertOverflows    int64 //超过监控阙值的记录数
	TotalCommitOverflows    int64 //超过监控阙值的记录数
	TotalInsertOverflowTime int64 //超过监控阙值的总执行时间 millseconds
	TotalCommitOverflowTIme int64 //超过监控阙值的总执行时间 millseconds

}

func FindTable(conn *sql.Conn, table *Table) (*Table, error) {
	//查看是否存在tabname
	var (
		tabname string
		sqlStr  string
		cnt     int
	)
	tabname = strings.ToUpper(table.Tabname)
	tabFields := strings.Split(tabname, ".")
	if len(tabFields) == 1 {
		//只有表名
		sqlStr = fmt.Sprintf("select count(*) from syscat.tables where tabname='%s'", strings.ToUpper(tabname))
	} else if len(tabFields) == 2 {
		//有表名和schema名
		sqlStr = fmt.Sprintf("select count(*) from syscat.tables where tabschema='%s' and tabname='%s'", strings.ToUpper(tabFields[0]), strings.ToUpper(tabFields[1]))
	} else {
		//表名有问题
		return nil, errors.New("tabname error:" + tabname)
	}
	row := conn.QueryRowContext(context.Background(), sqlStr)
	if err := row.Scan(&cnt); err != nil {
		return nil, err
	}
	if cnt == 0 {
		return nil, nil
	} else {
		//如果已经存在了该表，则需要生成表结构体
		t := new(Table)
		t.Tabname = table.Tabname
		var (
			tbspace   string
			colno     int
			colname   string
			coltype   ColType
			collength int
			PKColname string
		)
		tabFields := strings.Split(tabname, ".")
		if len(tabFields) == 1 {
			tabname = tabFields[0]
		} else if len(tabFields) == 2 {
			//不判断schema
			tabname = tabFields[1]
		}
		sql_pk := fmt.Sprintf("select colname from syscat.INDEXCOLUSE where INDSCHEMA||'.'||INDNAME in (select indschema||'.'||indname from syscat.indexes where UNIQUERULE='P' and tabname='%s')", strings.ToUpper(tabname))
		sql_columns := fmt.Sprintf("select COLNO,COLNAME,TYPENAME,LENGTH from syscat.columns where tabname='%s' order by colno asc with ur", tabname)
		sql_tbspace := fmt.Sprintf("select tbspace from syscat.tables where tabname='%s'", tabname)
		row := conn.QueryRowContext(context.Background(), sql_tbspace)
		cols := make([]*Column, 0)
		if err := row.Scan(&tbspace); err != nil {
			tbspace = ""
		}
		row = conn.QueryRowContext(context.Background(), sql_pk)
		if err = row.Scan(&PKColname); err != nil {
			PKColname = ""
		}
		var rows *sql.Rows
		if rows, err = conn.QueryContext(context.Background(), sql_columns); err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			if err = rows.Scan(&colno, &colname, &coltype, &collength); err != nil {
				return nil, err
			}
			cols = append(cols, &Column{COLNO: colno,
				COLNAME: colname,
				Type:    coltype,
				LENGTH:  collength,
				ISPK:    colname == PKColname,
			})
		}
		t.Tabname = tabname
		t.Tbspace = tbspace
		t.Cols = cols
		return t, nil
	}

}

func GenTable(tabname, tbspace string, colcount int, pk bool) *Table {
	rand.Seed(time.Now().Unix())
	self := new(Table)
	self.Tbspace = tbspace
	self.Tabname = tabname
	f := func(a, b int) int {
		r := rand.Intn(b)
		if r < a {
			return a
		} else {
			return r
		}
	}
	if colcount < 1 {
		return nil
	} else if colcount == 1 {
		self.Cols = append(self.Cols, &Column{COLNO: 1, COLNAME: "int_col1", ISPK: pk, Type: BIGINT})
	} else if colcount > 1 && colcount <= 10 {
		self.Cols = append(self.Cols, &Column{COLNO: 1, COLNAME: "int_col1", ISPK: pk, Type: BIGINT})
		for i := 1; i < colcount+1; i++ {
			self.Cols = append(self.Cols, &Column{COLNO: i, COLNAME: fmt.Sprintf("varchar_col%d", i), Type: VARCHAR, ISPK: false, LENGTH: f(5, 500)})
		}
	} else if colcount > 10 {
		self.Cols = append(self.Cols, &Column{COLNO: 1, COLNAME: "int_col1", ISPK: pk, Type: BIGINT})
		for i := 1; i < colcount/4; i++ {
			self.Cols = append(self.Cols, &Column{COLNO: i, COLNAME: fmt.Sprintf("char_col%d", i), Type: CHAR, ISPK: false, LENGTH: f(10, 255)})
		}
		for i := colcount / 4; i < colcount+1; i++ {
			self.Cols = append(self.Cols, &Column{COLNO: i, COLNAME: fmt.Sprintf("varchar_col%d", i), Type: VARCHAR, ISPK: false, LENGTH: f(10, 200)})
		}
	}

	return self
}

type ColType string

const (
	VARCHAR ColType = "VARCHAR"
	CHAR    ColType = "CHARACTER"
	INT     ColType = "INT"
	BIGINT  ColType = "BIGINT"
)

type Table struct {
	Tabname string
	Tbspace string
	Cols    []*Column
}

func (self *Table) CreateTableSQL() string {
	//col按照colno排序
	sort.Sort(Columns(self.Cols))
	Collist := make([]string, 0)
	for _, col := range self.Cols {
		Collist = append(Collist, col.CreateColSQL())
	}
	if self.Tbspace == "" {
		return fmt.Sprintf("create table %s (%s)", self.Tabname, strings.Join(Collist, ","))
	} else {
		return fmt.Sprintf("create table %s (%s) in %s", self.Tabname, strings.Join(Collist, ","), self.Tbspace)
	}
}

type Column struct {
	COLNAME string
	COLNO   int
	Type    ColType
	LENGTH  int
	ISPK    bool
}

func (self *Column) CreateColSQL() string {
	var (
		TypeStr string
		PKStr   string
	)
	switch self.Type {
	case VARCHAR:
		TypeStr = fmt.Sprintf("VARCHAR(%d)", self.LENGTH)
	case CHAR:
		TypeStr = fmt.Sprintf("CHARACTER(%d)", self.LENGTH)
	case INT:
		TypeStr = "INT"
	case BIGINT:
		TypeStr = "BIGINT"
	}
	switch self.ISPK {
	case true:
		PKStr = "not null primary key"
	case false:
		PKStr = ""
	}
	return fmt.Sprintf("%s %v %v", self.COLNAME, TypeStr, PKStr)
}

type Columns []*Column

func (c Columns) Len() int {
	return len(c)
}
func (c Columns) Less(i, j int) bool {
	return c[i].COLNO < c[j].COLNO
}
func (c Columns) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}
