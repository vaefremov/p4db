package p4db_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/vaefremov/p4db"
)

func TestContainerScalarAttr(t *testing.T) {
	db, err := p4db.Connect(DSN)
	if err != nil {
		panic(err)
	}
	var id int64 = 330087
	val, err := db.ContainerScalarAttr(id, "path")
	if err != nil {
		t.Error(err)
		return
	}
	res := val.String()
	fmt.Printf("Attr value: %v\n", res)
	if res != "test2_1188536112.19" {
		t.Error()
	}

	// D case
	id = 152290
	if val, err = db.ContainerScalarAttr(id, "cdpStep"); err != nil {
		t.Error(err)
		return
	}
	if resD, err := val.AsDouble(); err == nil {
		fmt.Printf("Attr value: %v\n", resD)
		if resD != 12.5 {
			t.Error()
		}
	} else {
		t.Error(err)
	}

	// I case
	id = 18521
	if val, err = db.ContainerScalarAttr(id, "nx"); err != nil {
		t.Error(err)
		return
	}
	if resD, err := val.AsInt(); err == nil {
		fmt.Printf("Attr value: %v\n", resD)
		if resD != 191 {
			t.Error()
		}
	} else {
		t.Error(err)
	}

	// R case
	id = 36474
	if val, err = db.ContainerScalarAttr(id, "Ref2doc1"); err != nil {
		t.Error(err)
		return
	}
	if resD, err := val.AsRef(); err == nil {
		fmt.Printf("Attr value: %v\n", resD)
		if resD != 36475 {
			t.Error()
		}
	} else {
		t.Error(err)
	}

	// P case
	id = 2201
	if val, err = db.ContainerScalarAttr(id, "coords"); err != nil {
		t.Error(err)
		return
	}
	if resD, err := val.AsPoint(); err == nil {
		exp := p4db.Point{955526.631944, 766664.251389, 0}
		fmt.Printf("Attr value: %v\n", resD)
		if math.Abs(resD.X-exp.X) > 1. || math.Abs(resD.Y-exp.Y) > 1. || math.Abs(resD.Z-exp.Z) > 1. {
			t.Error(math.Abs(resD.X-exp.X), math.Abs(resD.Y-exp.Y), math.Abs(resD.Z-exp.Z))
		}
	} else {
		t.Error(err)
	}

	return

	// X case
	id = 428394
	if val, err = db.ContainerScalarAttr(id, "boundaries"); err != nil {
		t.Error(err)
		return
	}
	if resD, err := val.AsDRefPair(); err == nil {
		exp := p4db.DRefPair{3406.48, 4467173}
		fmt.Printf("Attr value: %v", resD)
		if math.Abs(resD.ValD-exp.ValD) > 1. || resD.Ref != exp.Ref {
			t.Error(math.Abs(resD.ValD-exp.ValD), resD.Ref)
		}
	} else {
		t.Error(err)
	}

}

func TestContainerArrayAttribute(t *testing.T) {
	db, err := p4db.Connect(DSN)
	if err != nil {
		panic(err)
	}
	var id int64 = 14009832
	var val p4db.ArrayAttribute
	val, err = db.ContainerArrayAttr(id, "description1")
	if err != nil {
		t.Error(err)
		return
	}
	if resD, err := val.AsStringArr(); err == nil {
		fmt.Printf("Attr value: %#v", resD)
		if len(resD) != 6 {
			t.Error("Unexpected array length", len(resD))
			return
		}

	} else {
		t.Error(err)
	}

}

func ExampleContainerAttributes() {
	db, err := p4db.Connect(DSN)
	if err != nil {
		panic(err)
	}
	var id int64 = 330087
	if attrs, err := db.ContainerAttributes(id); err == nil {
		fmt.Printf("%v", attrs)
		// Output: map[path:qwerwqer/qwerwqer/qwerewqr]
	} else {
		panic(err)
	}
}
