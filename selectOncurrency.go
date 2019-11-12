package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/ibmdb/go_ibm_db"
	"log"
)

//发起并发查询SQL语句请求
/*
export DB2HOME=$DB2_HOME
export CGO_CFLAGS=-I$DB2HOME/include
export CGO_LDFLAGS=-L$DB2HOME/lib
export LD_LIBRARY_PATH=$DB2_HOME/lib
*/
func main() {
	var (
		num_q    int
		host     string
		port     int
		user     string
		password string
		dbname   string
		stmt     string
	)
	flag.StringVar(&host, "host", "localhost", "IP")
	flag.IntVar(&port, "port", 60000, "port")
	flag.StringVar(&dbname, "dbname", "sample", "database name")
	flag.StringVar(&user, "user", "db2inst1", "user name")
	flag.StringVar(&password, "pwd", "db2inst1", "password")
	flag.StringVar(&stmt, "sql", "", "sql text")
	flag.IntVar(&num_q, "parallel", 30, "parallel connections")
	flag.Parse()
	var db *sql.DB
	var err error
	conn := fmt.Sprintf("HOSTNAME=%s;PORT=%d;PROTOCOL=TCPIP;DATABASE=%s;UID=%s;PWD=%s", host, port, dbname, user, password)
	if db, err = sql.Open("go_ibm_db", conn); err != nil {
		log.Fatal(err)
	}
	if stmt == "" {
		panic("No sql")
	}
	for i := 0; i < num_q; i++ {
		go func() {
			for {
				if _, err := db.Exec(stmt); err != nil {
					break
				}
			}

		}()
	}
	select {}
}
