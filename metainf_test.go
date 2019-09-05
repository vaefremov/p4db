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
	db, err := p4db.New(DSN1)
	if err != nil {
		panic(err)
	}
	return db
}

func TestUpdateMetaInf(t *testing.T) {
	db, err := p4db.New(DSN1)
	if err != nil {
		t.Error(err.Error())
	}
	err = p4db.UpdateMetaInf(db)
	if err != nil {
		t.Error(err.Error())
	}

}

func TestIndexByName(t *testing.T) {
	_ = mustInitialize()
	ctype1 := "proj"
	attr1 := "coordinateSystem"
	ind, tStr, isArray, err := p4db.IndexByName(ctype1, attr1)
	if err != nil {
		t.Error(err)
	}
	wanted_ind := int16(209)
	if ind != wanted_ind {
		t.Errorf("wanted %v received %v ", wanted_ind, ind)
	}
	if isArray {
		t.Error(attr1 + " should be a scalar")
	}
	if tStr != p4db.C {
		t.Error(attr1 + " should be of type C")
	}
}

func TestAttributeNames(t *testing.T) {
	_ = mustInitialize()
	expRes := []string{"coordinateSystem", "path"}
	res := p4db.AttributeNames("proj")
	if !reflect.DeepEqual(expRes, res) {
		t.Errorf("wanted: %v received: %v", expRes, res)
	}
	expRes = []string{"cdp", "cdpStep", "geometry", "path", "refCDP", "refSP", "spDir"}
	res = p4db.AttributeNames("lin1")
	if !reflect.DeepEqual(expRes, res) {
		t.Errorf("wanted: %v received: %q", expRes, res)
	}

}

func ExampleGetContainerTypes() {
	_ = mustInitialize()
	t := p4db.ContainerTypes()
	k := "weld"
	fmt.Println(k, t[k])
	// Output: weld Simplified model well data

}

func ExampleGetTypesHierarchy() {
	_ = mustInitialize()
	tmp := p4db.TypesHierarchy()
	k := "wel1"
	v := tmp[k]
	fmt.Println(k, v)
	// Output: wel1 [aw2c aw2l cgr1 dirl doc2 par2 tgrp wbnd weld wtpl]
}

func TestCanCreateSubcontainer(t *testing.T) {
	_ = mustInitialize()
	cases := []struct {
		in1, in2 string
		exp      bool
	}{
		{"root", "proj", true},
		{"proj", "wel1", true},
		{"proj", "qqq", false},
		{"wel1", "weld", true},
	}
	for _, c := range cases {
		res := p4db.CanCreateSubcontainer(c.in1, c.in2)
		if res != c.exp {
			t.Error("Case failed", c)
		}
	}
}
