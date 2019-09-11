package p4db

import (
	"time"
)

func (db *P4db) SubContainersList(pid int64) (res []Container, err error) {
	res = []Container{}
	err = db.C.Select(&res, "select * from Containers where LinkUp=?", pid)
	return
}

func (db *P4db) CAttributes(cid int64) (res []dataValuesCDB, err error) {
	res = []dataValuesCDB{}
	err = db.C.Select(&res, "select * from DataValuesC where LinkContainer=? ", cid)
	return
}

func (db *P4db) IAttributes(cid int64) (res []dataValuesIDB, err error) {
	res = []dataValuesIDB{}
	err = db.C.Select(&res, "select * from DataValuesI where LinkContainer=?", cid)
	return
}

func (db *P4db) DAttributes(cid int64) (res []dataValuesCDB, err error) {
	res = []dataValuesCDB{}
	err = db.C.Select(&res, "select * from DataValuesD where LinkContainer=? ", cid)
	return
}

func (db *P4db) RAttributes(cid int64) (res []dataValuesRDB, err error) {
	res = []dataValuesRDB{}
	err = db.C.Select(&res, "select * from DataValuesR where LinkContainer=? ", cid)
	return
}

func (db *P4db) PAttributes(cid int64) (res []dataValuesPDB, err error) {
	res = []dataValuesPDB{}
	err = db.C.Select(&res, "select * from DataValuesP where LinkContainer=? ", cid)
	return
}

func (db *P4db) XAttributes(cid int64) (res []dataValuesXDB, err error) {
	res = []dataValuesXDB{}
	err = db.C.Select(&res, "select * from DataValuesX where LinkContainer=? ", cid)
	return
}

func (db *P4db) FAttributes(cid int64) (res []dataValuesIDB, err error) {
	res = []dataValuesIDB{}
	err = db.C.Select(&res, "select * from DataValuesF where LinkContainer=? ", cid)
	return
}

type changeLogDB struct {
	TableType string    `db:"TableType"`
	Operation string    `db:"Operation"`
	Modified  time.Time `db:"Modified"`
	UserID    int16     `db:"UserID"`
	Link      int64     `db:"Link"`
}

func (db *P4db) LogRecords(id int64, tableName string) (res []changeLogDB, err error) {
	res = []changeLogDB{}
	err = db.C.Select(&res, "select * from ChangeLog where Link=? and TableType=?", id, tableName)
	return
}
