package p4db

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type Container struct {
	CodeContainer    int64         `db:"CodeContainer"`
	LinkUp           int64         `db:"LinkUp"`
	TopParent        int64         `db:"TopParent"`
	ContainerTypeStr string        `db:"ContainerType"`
	Status           string        `db:"Status"`
	ContainerName    string        `db:"ContainerName"`
	OwnerId          sql.NullInt64 `db:"ownerID"`
	IsProtected      bool          `db:"isProtected"`
}

type P4db struct {
	C *sqlx.DB
}

// Connect acquires connection to MySQL from the pool and updates the MetaInf
// structures if needed
func Connect(dsn string) (res *P4db, err error) {
	conn, err := sqlx.Connect("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db := P4db{C: conn}
	err = UpdateMetaInf(&db)
	return &db, err
}

func MustConnect(dsn string) (res *P4db) {
	db, err := Connect(dsn)
	if err != nil {
		log.Panicln(err)
	}
	return db
}

func (db *P4db) GetContainerById(id int64) (res Container, err error) {
	conn := *db.C
	res = Container{}
	err = conn.Get(&res, "select * from Containers where CodeContainer=?", id)
	return
}

func (db *P4db) GetSubContainersListAll(pid int64, actualsOnly bool) (res []Container, err error) {
	conn := *db.C
	res = []Container{}
	if actualsOnly {
		err = conn.Select(&res, "select * from Containers where LinkUp=? and Status='Actual'", pid)
		return
	}
	err = conn.Select(&res, "select * from Containers where LinkUp=?", pid)
	return
}

func (db *P4db) GetSubContainersList(pid int64) (res []Container, err error) {
	res, err = db.GetSubContainersListAll(pid, true)
	return
}

func (db *P4db) GetSubContainersListByType(pid int64, typeStr string) (res []Container, err error) {
	conn := *db.C
	res = []Container{}
	err = conn.Select(&res, "select * from Containers where LinkUp=? and Status='Actual' and ContainerType=?", pid, typeStr)
	return
}

func (db *P4db) CreateContainer(pid int64, typeStr string, name string) (id int64, err error) {
	conn := *db.C
	if tx, err := conn.Begin(); err == nil {
		_, err := tx.Exec("lock tables Containers write")
		// defer tx.Exec("unlock tables")
		if err != nil {
			return 0, err
		}
		time.Sleep(10 * time.Second)
		tx.Exec("unlock tables")
		tx.Commit()
	}
	return
}

// Close connection and return it to the pool
func (db *P4db) Close() {
	db.C.Close()
}
