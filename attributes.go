package p4db

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

// Point represents a point in 3D space
type Point struct {
	X, Y, Z float64
}

func (p Point) String() string {
	return fmt.Sprintf("Point{%g, %g, %g}", p.X, p.Y, p.Z)
}

// DRefPair represents pair of double (e.g. measured depth in a well) and reference to an object
type DRefPair struct {
	ValD float64
	Ref  int64
}

func (p DRefPair) String() string {
	return fmt.Sprintf("DRefPair{%g, %d}", p.ValD, p.Ref)
}

type Attribute interface {
	String() string
	AsInt() (int, error)
	AsDouble() (float64, error)
	AsRef() (int64, error)
	AsPoint() (Point, error)
	AsDRefPair() (DRefPair, error)
	ToArray() (ArrayAttribute, bool)
}

type ArrayAttribute interface {
	String() string
	AsStringArr() ([]string, error)
	AsIntArr() ([]int, error)
	AsDoubleArr() ([]float64, error)
	AsRefArr() ([]int64, error)
	AsPointArr() ([]Point, error)
	AsDRefPairArr() ([]DRefPair, error)
}

type dataValuesDB struct {
	CodeValue     int64  `db:"CodeValue"`
	LinkContainer int64  `db:"LinkContainer"`
	LinkMetaData  int64  `db:"LinkMetaData"`
	ValueIndex    int32  `db:"ValueIndex"`
	Status        string `db:"Status"`
}

type dataValuesCDB struct {
	dataValuesDB
	DataValue sql.NullString `db:"DataValue"`
}

type dataValuesIDB struct {
	dataValuesDB
	DataValue int64 `db:"DataValue"`
}

type dataValuesDDB struct {
	dataValuesDB
	DataValue float64 `db:"DataValue"`
}

type dataValuesRDB struct {
	dataValuesDB
	DataValue int64 `db:"DataValue"`
}

type dataValuesPDB struct {
	dataValuesDB
	DataValueX float64 `db:"DataValueX"`
	DataValueY float64 `db:"DataValueY"`
	DataValueZ float64 `db:"DataValueZ"`
}

type dataValuesXDB struct {
	dataValuesDB
	DataValueD float64 `db:"DataValueD"`
	DataValueR int64   `db:"DataValueR"`
}

// ========= C case =============

type CAttr struct {
	Val string
}

func (a CAttr) String() string {
	return a.Val
}

func (a CAttr) AsInt() (int, error) {
	return 0, fmt.Errorf("conversion error")
}

func (a CAttr) AsDouble() (float64, error) {
	return 0, fmt.Errorf("conversion error")
}

func (a CAttr) AsRef() (int64, error) {
	return 0, fmt.Errorf("conversion error")
}
func (a CAttr) AsPoint() (Point, error) {
	return Point{}, fmt.Errorf("conversion error")
}
func (a CAttr) AsDRefPair() (DRefPair, error) {
	return DRefPair{}, fmt.Errorf("conversion error")
}

func (a CAttr) ToArray() (ArrayAttribute, bool) {
	return nil, false
}

// ========= I case =============

type IAttr struct {
	Val int
}

func (a IAttr) String() string {
	return fmt.Sprintf("%d", a.Val)
}

func (a IAttr) AsInt() (int, error) {
	return a.Val, nil
}

func (a IAttr) AsDouble() (float64, error) {
	return 0, fmt.Errorf("conversion error")
}

func (a IAttr) AsRef() (int64, error) {
	return 0, fmt.Errorf("conversion error")
}

func (a IAttr) AsPoint() (Point, error) {
	return Point{}, fmt.Errorf("conversion error")
}
func (a IAttr) AsDRefPair() (DRefPair, error) {
	return DRefPair{}, fmt.Errorf("conversion error")
}

func (a IAttr) ToArray() (ArrayAttribute, bool) {
	return nil, false
}

// ========= D case =============

type DAttr struct {
	Val float64
}

func (a DAttr) String() string {
	return fmt.Sprintf("%g", a.Val)
}

func (a DAttr) AsInt() (int, error) {
	return 0, fmt.Errorf("conversion error")
}

func (a DAttr) AsDouble() (float64, error) {
	return a.Val, nil
}

func (a DAttr) AsRef() (int64, error) {
	return 0, fmt.Errorf("conversion error")
}

func (a DAttr) AsPoint() (Point, error) {
	return Point{}, fmt.Errorf("conversion error")
}
func (a DAttr) AsDRefPair() (DRefPair, error) {
	return DRefPair{}, fmt.Errorf("conversion error")
}

func (a DAttr) ToArray() (ArrayAttribute, bool) {
	return nil, false
}

// ========= R case =============

type RAttr struct {
	Val int64
}

func (a RAttr) String() string {
	return fmt.Sprintf("Ref:%d", a.Val)
}

func (a RAttr) AsInt() (int, error) {
	return 0, fmt.Errorf("conversion error")
}

func (a RAttr) AsDouble() (float64, error) {
	return 0, fmt.Errorf("conversion error")
}

func (a RAttr) AsRef() (int64, error) {
	return a.Val, nil
}

func (a RAttr) AsPoint() (Point, error) {
	return Point{}, fmt.Errorf("conversion error")
}
func (a RAttr) AsDRefPair() (DRefPair, error) {
	return DRefPair{}, fmt.Errorf("conversion error")
}

func (a RAttr) ToArray() (ArrayAttribute, bool) {
	return nil, false
}

// ========= P case =============

type PAttr struct {
	ValX, ValY, ValZ float64
}

func (a PAttr) String() string {
	return fmt.Sprintf("Point{%g, %g, %g}", a.ValX, a.ValY, a.ValZ)
}

func (a PAttr) AsInt() (int, error) {
	return 0, fmt.Errorf("conversion error")
}

func (a PAttr) AsDouble() (float64, error) {
	return 0, fmt.Errorf("conversion error")
}

func (a PAttr) AsRef() (int64, error) {
	return 0, fmt.Errorf("conversion error")
}

func (a PAttr) AsPoint() (Point, error) {
	return Point{a.ValX, a.ValY, a.ValZ}, nil
}
func (a PAttr) AsDRefPair() (DRefPair, error) {
	return DRefPair{}, fmt.Errorf("conversion error")
}

func (a PAttr) ToArray() (ArrayAttribute, bool) {
	return nil, false
}

// ========= X case =============

type XAttr struct {
	ValD float64
	ValR int64
}

func (a XAttr) String() string {
	return fmt.Sprintf("DRefPair{%g, %d}", a.ValD, a.ValR)
}

func (a XAttr) AsInt() (int, error) {
	return 0, fmt.Errorf("conversion error")
}

func (a XAttr) AsDouble() (float64, error) {
	return 0, fmt.Errorf("conversion error")
}

func (a XAttr) AsRef() (int64, error) {
	return 0, fmt.Errorf("conversion error")
}

func (a XAttr) AsPoint() (Point, error) {
	return Point{}, fmt.Errorf("conversion error")
}
func (a XAttr) AsDRefPair() (DRefPair, error) {
	return DRefPair{a.ValD, a.ValR}, nil
}

func (a XAttr) ToArray() (ArrayAttribute, bool) {
	return nil, false
}

// ================ End of scalar attribute type definitions
//   ====== C ===========
type CAttrArr struct {
	Val []string
}

func (a CAttrArr) String() string {
	return fmt.Sprintf("%v", a.Val)
}

func (a CAttrArr) AsStringArr() ([]string, error) {
	return a.Val, nil
}

func (a CAttrArr) AsIntArr() ([]int, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a CAttrArr) AsDoubleArr() ([]float64, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a CAttrArr) AsRefArr() ([]int64, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a CAttrArr) AsPointArr() ([]Point, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a CAttrArr) AsDRefPairArr() ([]DRefPair, error) {
	return nil, fmt.Errorf("array conversion error")
}

//   ====== I ===========
type IAttrArr struct {
	Val []int
}

func (a IAttrArr) String() string {
	return fmt.Sprintf("%v", a.Val)
}

func (a IAttrArr) AsStringArr() ([]string, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a IAttrArr) AsIntArr() ([]int, error) {
	return a.Val, nil
}

func (a IAttrArr) AsDoubleArr() ([]float64, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a IAttrArr) AsRefArr() ([]int64, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a IAttrArr) AsPointArr() ([]Point, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a IAttrArr) AsDRefPairArr() ([]DRefPair, error) {
	return nil, fmt.Errorf("array conversion error")
}

//   ====== D ===========
type DAttrArr struct {
	Val []float64
}

func (a DAttrArr) String() string {
	return fmt.Sprintf("%v", a.Val)
}

func (a DAttrArr) AsStringArr() ([]string, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a DAttrArr) AsIntArr() ([]int, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a DAttrArr) AsDoubleArr() ([]float64, error) {
	return a.Val, nil
}

func (a DAttrArr) AsRefArr() ([]int64, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a DAttrArr) AsPointArr() ([]Point, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a DAttrArr) AsDRefPairArr() ([]DRefPair, error) {
	return nil, fmt.Errorf("array conversion error")
}

//   ====== R ===========
type RAttrArr struct {
	Val []int64
}

func (a RAttrArr) String() string {
	return fmt.Sprintf("%v", a.Val)
}

func (a RAttrArr) AsStringArr() ([]string, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a RAttrArr) AsIntArr() ([]int, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a RAttrArr) AsDoubleArr() ([]float64, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a RAttrArr) AsRefArr() ([]int64, error) {
	return a.Val, nil
}

func (a RAttrArr) AsPointArr() ([]Point, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a RAttrArr) AsDRefPairArr() ([]DRefPair, error) {
	return nil, fmt.Errorf("array conversion error")
}

//   ====== R ===========
type PAttrArr struct {
	Val []Point
}

func (a PAttrArr) String() string {
	return fmt.Sprintf("%v", a.Val)
}

func (a PAttrArr) AsStringArr() ([]string, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a PAttrArr) AsIntArr() ([]int, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a PAttrArr) AsDoubleArr() ([]float64, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a PAttrArr) AsRefArr() ([]int64, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a PAttrArr) AsPointArr() ([]Point, error) {
	return a.Val, nil
}

func (a PAttrArr) AsDRefPairArr() ([]DRefPair, error) {
	return nil, fmt.Errorf("array conversion error")
}

//   ====== X ===========
type XAttrArr struct {
	Val []DRefPair
}

func (a XAttrArr) String() string {
	return fmt.Sprintf("%v", a.Val)
}

func (a XAttrArr) AsStringArr() ([]string, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a XAttrArr) AsIntArr() ([]int, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a XAttrArr) AsDoubleArr() ([]float64, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a XAttrArr) AsRefArr() ([]int64, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a XAttrArr) AsPointArr() ([]Point, error) {
	return nil, fmt.Errorf("array conversion error")
}

func (a XAttrArr) AsDRefPairArr() ([]DRefPair, error) {
	return a.Val, nil
}

// ================ End of array attribute type definitions

func (db *P4db) ContainerScalarAttr(id int64, attrName string) (attr Attribute, err error) {
	c, err := db.GetContainerById(id)
	if err != nil {
		return nil, err
	}
	ind, t, isArray, err := IndexByName(c.ContainerTypeStr, attrName)
	if err != nil {
		return nil, err
	}
	if isArray {
		return nil, fmt.Errorf("a scalar attribute expected")
	}
	switch t {
	case C:
		log.Println("C case of attribute")
		var v dataValuesCDB
		if err = db.C.Get(&v, "select * from DataValuesC where LinkMetaData=? and LinkContainer=? and Status='Actual'", ind, id); err == nil {
			attr = CAttr{Val: v.DataValue.String}
			return
		}
	case I:
		log.Println("I case of attribute")
		var v dataValuesIDB
		if err = db.C.Get(&v, "select * from DataValuesI where LinkMetaData=? and LinkContainer=? and Status='Actual'", ind, id); err == nil {
			attr = IAttr{Val: int(v.DataValue)}
			return
		}
	case D:
		log.Println("D case of attribute")
		var v dataValuesDDB
		if err = db.C.Get(&v, "select * from DataValuesD where LinkMetaData=? and LinkContainer=? and Status='Actual'", ind, id); err == nil {
			attr = DAttr{Val: v.DataValue}
			return
		}
	case R:
		log.Println("R case of attribute")
		var v dataValuesRDB
		if err = db.C.Get(&v, "select * from DataValuesR where LinkMetaData=? and LinkContainer=? and Status='Actual'", ind, id); err == nil {
			attr = RAttr{Val: v.DataValue}
			return
		}
	case P:
		log.Println("P case of attribute")
		var v dataValuesPDB
		if err = db.C.Get(&v, "select * from DataValuesP where LinkMetaData=? and LinkContainer=? and Status='Actual'", ind, id); err == nil {
			attr = PAttr{ValX: v.DataValueX, ValY: v.DataValueY, ValZ: v.DataValueZ}
			return
		}
	case X:
		log.Println("P case of attribute")
		var v dataValuesXDB
		if err = db.C.Get(&v, "select * from DataValuesX where LinkMetaData=? and LinkContainer=? and Status='Actual'", ind, id); err == nil {
			attr = XAttr{ValD: v.DataValueD, ValR: v.DataValueR}
			return
		}
	default:
		log.Println("Unsupported attribute type")
		panic("Unsupported attribute type " + t)
	}
	return nil, err
}

func (db *P4db) ContainerArrayAttr(id int64, attrName string) (attr ArrayAttribute, err error) {
	c, err := db.GetContainerById(id)
	if err != nil {
		return nil, err
	}
	ind, t, isArray, err := IndexByName(c.ContainerTypeStr, attrName)
	if err != nil {
		return nil, err
	}
	if !isArray {
		return nil, fmt.Errorf("an array attribute expected")
	}
	switch t {
	case C:
		log.Println("C case of attribute")
		v := []dataValuesCDB{}
		if err = db.C.Select(&v, "select * from DataValuesC where LinkMetaData=? and LinkContainer=? and Status='Actual'", ind, id); err == nil {
			res := make([]string, len(v))
			for i, x := range v {
				res[i] = x.DataValue.String
			}
			attr = CAttrArr{Val: res}
			return
		}
	case I:
		log.Println("I case of attribute")
		v := []dataValuesIDB{}
		if err = db.C.Select(&v, "select * from DataValuesI where LinkMetaData=? and LinkContainer=? and Status='Actual'", ind, id); err == nil {
			res := make([]int, len(v))
			for i, x := range v {
				res[i] = int(x.DataValue)
			}
			attr = IAttrArr{Val: res}
			return
		}
	case D:
		log.Println("D case of attribute")
		v := []dataValuesDDB{}
		if err = db.C.Select(&v, "select * from DataValuesD where LinkMetaData=? and LinkContainer=? and Status='Actual'", ind, id); err == nil {
			res := make([]float64, len(v))
			for i, x := range v {
				res[i] = x.DataValue
			}
			attr = DAttrArr{Val: res}
			return
		}
	case R:
		log.Println("R case of attribute")
		v := []dataValuesRDB{}
		if err = db.C.Select(&v, "select * from DataValuesR where LinkMetaData=? and LinkContainer=? and Status='Actual'", ind, id); err == nil {
			res := make([]int64, len(v))
			for i, x := range v {
				res[i] = x.DataValue
			}
			attr = RAttrArr{Val: res}
			return
		}
	case P:
		log.Println("P case of attribute")
		v := []dataValuesPDB{}
		if err = db.C.Select(&v, "select * from DataValuesP where LinkMetaData=? and LinkContainer=? and Status='Actual'", ind, id); err == nil {
			res := make([]Point, len(v))
			for i, x := range v {
				res[i] = Point{X: x.DataValueX, Y: x.DataValueY, Z: x.DataValueZ}
			}
			attr = PAttrArr{Val: res}
			return
		}
	case X:
		log.Println("P case of attribute")
		v := []dataValuesXDB{}
		if err = db.C.Select(&v, "select * from DataValuesX where LinkMetaData=? and LinkContainer=? and Status='Actual'", ind, id); err == nil {
			res := make([]DRefPair, len(v))
			for i, x := range v {
				res[i] = DRefPair{ValD: x.DataValueD, Ref: x.DataValueR}
			}
			attr = XAttrArr{Val: res}
			return
		}
	default:
		log.Println("Unsupported attribute type", t)
		err = fmt.Errorf("unsupported attribute type: %s", t)
	}
	return nil, err
}

func (db *P4db) ContainerAttributes(id int64) (map[string]Attribute, error) {
	log.Println(id)
	res := make(map[string]Attribute)
	res["path"] = CAttr{Val: "qwerwqer/qwerwqer/qwerewqr"}
	return res, nil
}
