package p4db_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/vaefremov/p4db"
)

const (
	DSN = "panadm:pan123@tcp(192.168.4.46:3306)/PANGEA?allowOldPasswords=1&parseTime=true&charset=utf8"
)

func TestConnect(t *testing.T) {
	db, err := p4db.Connect(DSN)
	defer db.Close()
	if err != nil {
		t.Error(err.Error())
	}
	conn := *db.C
	cont := p4db.Container{}
	err = conn.Get(&cont, "select * from Containers where CodeContainer = 1")
	if err != nil {
		t.Error(err.Error())
	}
}

func TestGetContainerById(t *testing.T) {
	db := p4db.MustConnect(DSN)
	defer db.Close()
	id := int64(1)
	cont, err := db.GetContainerById(id)
	if cont.CodeContainer != id {
		t.Fatal("Wrong container returned", err.Error())
	}
	if cont.ContainerName != "PANGEA Inc." {
		t.Fatal("Wrong repository name", cont.ContainerName)
	}
}

func TestGetSubContainersListAll(t *testing.T) {
	db := p4db.MustConnect(DSN)
	defer db.Close()
	conts, err := db.GetSubContainersListAll(1, false)
	if err != nil {
		t.Error(err.Error())
	}
	for i, c := range conts {
		fmt.Println(i, c)
	}
}
func TestGetSubContainersList(t *testing.T) {
	db := p4db.MustConnect(DSN)
	defer db.Close()
	conts, err := db.GetSubContainersList(1)
	if err != nil {
		t.Error(err.Error())
	}
	for i, c := range conts {
		if c.Status != "Actual" {
			t.Fatalf("Wrong status: %#v, should be Actual", c)
		}
		fmt.Println(i, c)
	}
}

func TestGetSubContainersListByType(t *testing.T) {
	db := p4db.MustConnect(DSN)
	defer db.Close()
	typeStr := "lin1"
	conts, err := db.GetSubContainersListByType(3, typeStr)
	if err != nil {
		t.Error(err.Error())
	}
	for i, c := range conts {
		if c.Status != "Actual" {
			t.Fatalf("Wrong status: %#v, should be Actual", c)
		}
		if c.ContainerTypeStr != typeStr {
			t.Fatalf("Wrong type str: %#v, should be %s", c, typeStr)
		}
		fmt.Println(i, c)
	}
	// t.Error()
}

func TestCreateContainer(t *testing.T) {
	db := p4db.MustConnect(DSN)
	_, err := db.CreateContainer(1, "qqq", "Some name")
	if err != nil {
		t.Error(err.Error())
	}
	time.Sleep(5 * time.Second)
}
