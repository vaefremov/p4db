package p4db

import (
	"database/sql"
	"sync"

	"github.com/pkg/errors"
)

const (
	C = "C"
	I = "I"
	D = "D"
	F = "F"
	P = "P"
	R = "R"
	X = "X"
)

type ContainerAndAttributeNames struct {
	ContainerType string
	Name          string
}
type AttributeDescr struct {
	Id int16
	ContainerAndAttributeNames
	Type           string
	IsArray        bool
	ReferencedType string
}

type metaDataDB struct {
	CodeData                int16          `db:"CodeData"`
	ContainerType           string         `db:"ContainerType"`
	KeyWord                 string         `db:"KeyWord"`
	TypeData                string         `db:"TypeData"`
	DotPosition             int            `db:"DotPosition"`
	Dimension               int            `db:"Dimension"`
	ReferencedContainerType sql.NullString `db:"ReferencedContainerType"`
	LinkPermission          string         `db:"LinkPermission"`
}

type containerTypesDB struct {
	CodeContainerType string `db:"CodeContainerType"`
	NameContainerType string `db:"NameContainerType"`
}

type containerTypesSubmissionDB struct {
	ContainerTypeMaster string `db:"ContainerTypeMaster"`
	ContainerTypeSlave  string `db:"ContainerTypeSlave"`
}

var (
	mu             sync.Mutex
	containerTypes map[string]string
	typeHierarchy  map[string][]string
	attributes     map[ContainerAndAttributeNames][]AttributeDescr
	isValid        bool
)

func UpdateMetaInf(db *P4db) (err error) {
	mu.Lock()
	defer mu.Unlock()
	if !isValid {
		err = fillContainerTypes(db)
		if err != nil {
			return
		}
		err = fillTypeHierarchy(db)
		if err != nil {
			return
		}
		err = fillAttributes(db)
		if err != nil {
			return
		}
	}
	return
}

func fillAttributes(db *P4db) (err error) {
	attributes = make(map[ContainerAndAttributeNames][]AttributeDescr)
	tmp := []metaDataDB{}
	if err = db.C.Select(&tmp, "select CodeData, ContainerType, KeyWord, TypeData, Dimension, ReferencedContainerType, LinkPermission  from MetaData"); err == nil {
		for _, m := range tmp {
			descr := AttributeDescr{m.CodeData, ContainerAndAttributeNames{m.ContainerType, m.KeyWord}, m.TypeData, m.Dimension > 0, m.ReferencedContainerType.String}
			containerAttribute := ContainerAndAttributeNames{m.ContainerType, m.KeyWord}
			attributes[containerAttribute] = append(attributes[containerAttribute], descr)
		}
	}
	return err
}

func fillContainerTypes(db *P4db) (err error) {
	containerTypes = make(map[string]string)
	tmp := []containerTypesDB{}
	if err = db.C.Select(&tmp, "select * from ContainerTypes"); err == nil {
		for _, t := range tmp {
			containerTypes[t.CodeContainerType] = t.NameContainerType
		}
	}
	return
}

func fillTypeHierarchy(db *P4db) (err error) {
	typeHierarchy = make(map[string][]string)
	tmp := []containerTypesSubmissionDB{}
	if err = db.C.Select(&tmp, "select * from ContainerTypeSubmission"); err == nil {
		for _, t := range tmp {
			typeHierarchy[t.ContainerTypeMaster] = append(typeHierarchy[t.ContainerTypeMaster], t.ContainerTypeSlave)
		}
	}
	return
}

// Basic requests

func IndexByName(typeStr string, attributeName string) (ind int16, err error) {
	ind, err = -1, errors.New("unknown error")
	return
}

func ContainerTypes() map[string]string {
	cp := make(map[string]string)
	for k, v := range containerTypes {
		cp[k] = v
	}
	return cp
}

func TypesHierarchy() map[string][]string {
	cp := make(map[string][]string)
	for k, v := range typeHierarchy {
		newList := make([]string, len(v))
		copy(newList, v)
		cp[k] = newList
	}
	return cp
}