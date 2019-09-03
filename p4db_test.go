package p4db_test

import (
	"fmt"
	"testing"

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
	counter := map[string]int{}
	for _, c := range conts {
		// fmt.Println(i, c)
		counter[c.Status] += 1
	}
	if counter["Actual"] == 0 || counter["Deleted"] == 0 {
		fmt.Printf("Counter: %v", counter)
		t.Error("no deleted or actual projects")
	}
}
func TestGetSubContainersList(t *testing.T) {
	db := p4db.MustConnect(DSN)
	defer db.Close()
	conts, err := db.GetSubContainersList(1)
	if err != nil {
		t.Error(err.Error())
	}
	for _, c := range conts {
		if c.Status != "Actual" {
			t.Fatalf("Wrong status: %#v, should be Actual", c)
		}
		// fmt.Println(i, c)
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
	for _, c := range conts {
		if c.Status != "Actual" {
			t.Fatalf("Wrong status: %#v, should be Actual", c)
		}
		if c.ContainerTypeStr != typeStr {
			t.Fatalf("Wrong type str: %#v, should be %s", c, typeStr)
		}
		// fmt.Println(i, c)
	}
	// t.Error()
}
func TestGetSubContainersListByTypeWc(t *testing.T) {
	db := p4db.MustConnect(DSN)
	defer db.Close()
	typeStr := "proj"
	conts, err := db.GetSubContainersListByTypeWc(1, typeStr, "test_%")
	if err != nil {
		t.Error(err.Error())
	}
	for _, c := range conts {
		if c.Status != "Actual" {
			t.Fatalf("Wrong status: %#v, should be Actual", c)
		}
		if c.ContainerTypeStr != typeStr {
			t.Fatalf("Wrong type str: %#v, should be %s", c, typeStr)
		}
		// fmt.Println(c)
	}
}

func TestCreateContainer(t *testing.T) {
	db := p4db.MustConnect(DSN)
	_, err := db.CreateContainer(1, "qqq", "Some name")
	if err != nil {
		t.Error(err.Error())
	}
	// t.Error("Not implemented!")
}

func TestProjectsNamePath(t *testing.T) {
	db := p4db.MustConnect(DSN)
	if tmp, err := db.ProjectsNamePath(); err == nil {
		fmt.Println(tmp)
	} else {
		t.Error(err)
	}
	// t.Error()
}

func TestSubContainersListWithCAttributeByType(t *testing.T) {
	db := p4db.MustConnect(DSN)
	if tmp, err := db.SubContainersListWithCAttributeByType(1, "proj", "path"); err == nil {
		fmt.Println(tmp)
	} else {
		t.Error(err)
	}
	// t.Error()
}
