package p4db_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/vaefremov/p4db"
)

const (
	DSN1 = "panadm:pan123@tcp(192.168.4.46:3306)/PANGEA?allowOldPasswords=1&parseTime=true&charset=utf8"
)

func mustInitialize() *p4db.P4db {
	db, err := p4db.Connect(DSN1)
	if err != nil {
		panic(err)
	}
	return db
}

func TestUpdateMetaInf(t *testing.T) {
	db, err := p4db.Connect(DSN1)
	defer db.Close()
	if err != nil {
		t.Error(err.Error())
	}
	err = p4db.UpdateMetaInf(db)
	if err != nil {
		t.Error(err.Error())
	}

}

func TestIndexByName(t *testing.T) {
	db := mustInitialize()
	defer db.Close()
	err := p4db.UpdateMetaInf(db)
	if err != nil {
		t.Error(err)
	}
	ind, err := p4db.IndexByName("proj", "coordinateSystem")
	wanted_ind := int16(209)
	if ind != wanted_ind {
		t.Error("wanted " + string(wanted_ind) + " received " + string(ind))
	}
}

func TestAttributeNames(t *testing.T) {
	db := mustInitialize()
	defer db.Close()
	err := p4db.UpdateMetaInf(db)
	if err != nil {
		t.Error(err)
	}
	expRes := []string{"coordinateSystem", "path"}
	res, _ := p4db.AttributeNames("proj")
	if !reflect.DeepEqual(expRes, res) {
		t.Errorf("wanted: %v received: %v", expRes, res)
	}
}

func ExampleGetContainerTypes() {
	db := mustInitialize()
	defer db.Close()
	err := p4db.UpdateMetaInf(db)
	if err != nil {
		panic(err)
	}
	t := p4db.ContainerTypes()
	k := "weld"
	fmt.Println(k, t[k])
	// Output: weld Simplified model well data

}

func ExampleGetTypesHierarchy() {
	db := mustInitialize()
	defer db.Close()
	err := p4db.UpdateMetaInf(db)
	if err != nil {
		panic(err)
	}
	tmp := p4db.TypesHierarchy()
	k := "wel1"
	v := tmp[k]
	fmt.Println(k, v)
	// Output: wel1 [aw2c aw2l cgr1 dirl doc2 par2 tgrp wbnd weld wtpl]
}
