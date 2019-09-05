package p4db

import (
	"database/sql"
	"log"
	"sync"

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

const REPOSITORY_ID = int64(1)

type P4db struct {
	C *sqlx.DB
}

var (
	p4dbConn P4db
	p4dbConnErr error
	createConnOnce sync.Once
)

// Connect acquires connection to MySQL from the pool and updates the MetaInf
// structures if needed
func New(dsn string) (res *P4db, err error) {
	createConnOnce.Do(func() {
		conn, err := sqlx.Connect("mysql", dsn)
		if err != nil {
			p4dbConnErr = err
			return
		}
		log.Println("Db connections pool initialized")
		p4dbConn = P4db{C: conn}
		p4dbConnErr = UpdateMetaInf(&p4dbConn)
	} )
	return &p4dbConn, p4dbConnErr
}

func MustNew(dsn string) (res *P4db) {
	db, err := New(dsn)
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

func (db *P4db) GetSubContainersListByTypeWc(pid int64, typeStr string, wc string) (res []Container, err error) {
	conn := *db.C
	res = []Container{}
	err = conn.Select(&res, "select * from Containers where LinkUp=? and Status='Actual' and ContainerType=? and ContainerName like ?", pid, typeStr, wc)
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
		// time.Sleep(10 * time.Second)
		tx.Exec("unlock tables")
		tx.Commit()
	}
	return
}

// Close connections pool
func (db *P4db) ClosePull() {
	db.C.Close()
}

// ======= Special purpose functions

type NamePath struct {
	Id   int64
	Name string
	Path string
}

func (db *P4db) ProjectsNamePath() (res []NamePath, err error) {
	tmp, err := db.SubContainersListWithCAttributeByType(REPOSITORY_ID, "proj", "path")
	if err != nil {
		return
	}
	res = make([]NamePath, len(tmp))
	for i, p := range tmp {
		res[i] = NamePath{Id: p.CodeContainer, Name: p.ContainerName, Path: p.CAttr}
	}
	return
}

type ContainerAndPath struct {
	CodeContainer    int64         `db:"CodeContainer"`
	ContainerTypeStr string        `db:"ContainerType"`
	ContainerName    string        `db:"ContainerName"`
	OwnerID          sql.NullInt64 `db:"ownerID"`
	IsProtected      bool          `db:"isProtected"`
	CAttr            string        `db:"DataValue"`
}

func (db *P4db) SubContainersListWithCAttributeByType(pid int64, typeStr, attrName string) (res []ContainerAndPath, err error) {
	attrID, _, _, err := IndexByName(typeStr, attrName)
	if err != nil {
		return
	}
	conn := *db.C
	res = []ContainerAndPath{}
	sqlTmpl := `select c.CodeContainer, c.ContainerType, c.ContainerName, c.ownerID, c.isProtected, d.DataValue  
	from Containers c left join DataValuesC d on c.CodeContainer=d.LinkContainer 
	where c.Status='Actual' and d.Status = 'Actual' and c.LinkUp=? and c.ContainerType=? and d.LinkMetaData=?
	`
	err = conn.Select(&res, sqlTmpl, pid, typeStr, attrID)
	return
}
