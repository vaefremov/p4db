package p4db_test

import (
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/vaefremov/p4db"
)

func TestContainerScalarAttr(t *testing.T) {
	db, err := p4db.New(DSN)
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
}

func TestTimeAttributes(t *testing.T) {
	db, err := p4db.New(DSN)
	if err != nil {
		panic(err)
	}
	var id int64 = 2248
	val, err := db.ContainerScalarAttr(id, "start")
	if err != nil {
		t.Error(err)
		return
	}
	exp := "2007-08-15 16:15:36"
	tm := val.(p4db.TAttr).Val
	res := tm.Format("2006-01-02 15:04:05")
	fmt.Println(tm.Format("2006-01-02 15:04:05"))
	fmt.Println(tm)
	fmt.Println("Attr value: ", val)
	if res != exp {
		t.Fatal(exp, res)
	}
	if val.String() != exp {
		t.Fatal("Conversion to string failed")
	}

}

func TestFixedPointAttributes(t *testing.T) {
	db, err := p4db.New(DSN)
	if err != nil {
		panic(err)
	}
	var id int64 = 2150
	val, err := db.ContainerScalarAttr(id, "XShift")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("xshift", val)

	val, err = db.ContainerScalarAttr(id, "YShift")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("yshift", val)

	val, err = db.ContainerScalarAttr(id, "zcompression")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("zcompression", val)

	id = 9621549
	val, err = db.ContainerScalarAttr(id, "XShift")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("xshift", val)
	// t.Error()
}

func TestContainerArrayAttribute(t *testing.T) {
	db, err := p4db.New(DSN)
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
		fmt.Printf("Attr value: %#v\n", resD)
		if len(resD) != 6 {
			t.Error("Unexpected array length", len(resD))
			return
		}

	} else {
		t.Error(err)
	}

	// I case
	id = 13
	if val, err = db.ContainerArrayAttr(id, "CDP"); err != nil {
		t.Error(err)
		return
	}
	if resD, err := val.AsIntArr(); err == nil {
		exp := [...]int{1, 31, 32}
		fmt.Printf("Attr value: %v\n", resD)
		for i, x := range exp {
			if x != resD[i] {
				t.Error(i, exp[i], resD[i])
			}
		}
	} else {
		t.Error(err)
	}
	// D case
	id = 8312990
	if val, err = db.ContainerArrayAttr(id, "filterParameters"); err != nil {
		t.Error(err)
		return
	}
	if resD, err := val.AsDoubleArr(); err == nil {
		fmt.Printf("Attr value: %v\n", resD)
		exp := []float64{10, 20, 70, 80}

		if !reflect.DeepEqual(exp, resD) {
			fmt.Printf("%T %T\n", exp, resD)
			t.Error(exp, resD)
		}
	} else {
		t.Error(err)
	}

	// R case
	id = 1294
	if val, err = db.ContainerArrayAttr(id, "Refs2horl"); err != nil {
		t.Error(err)
		return
	}
	if resD, err := val.AsRefArr(); err == nil {
		fmt.Printf("Attr value: %v\n", resD)
		exp := [...]int64{277, 273, 269, 265, 261}

		if !reflect.DeepEqual(exp[:5], resD[:5]) {
			fmt.Printf("%T %T\n", exp, resD)
			t.Error(exp, resD)
		}
	} else {
		t.Error(err)
	}

	// P case
	id = 13
	if val, err = db.ContainerArrayAttr(id, "Geometry"); err != nil {
		t.Error(err)
		return
	}
	if resD, err := val.AsPointArr(); err == nil {
		fmt.Printf("Attr value: %v\n", resD)
		exp := p4db.Point{1000250, 735725, 0}

		if !reflect.DeepEqual(exp, resD[0]) {
			fmt.Printf("%T %T\n", exp, resD)
			t.Error(exp, resD)
		}
	} else {
		t.Error(err)
	}

	// X case
	id = 428394
	if val, err = db.ContainerArrayAttr(id, "boundaries"); err != nil {
		t.Error(err)
		return
	}
	if resD, err := val.AsDRefPairArr(); err == nil {
		exp := p4db.DRefPair{3406.48, 4467173}
		fmt.Printf("Attr value: %v\n", resD)
		if math.Abs(resD[0].ValD-exp.ValD) > 1. || resD[0].Ref != exp.Ref {
			t.Error(math.Abs(resD[0].ValD-exp.ValD), resD[0].Ref)
		}
	} else {
		t.Error(err)
	}

}

func ExampleContainerAttributes() {
	db, err := p4db.New(DSN)
	if err != nil {
		panic(err)
	}
	var id int64 = 330087
	if attrs, err := db.ContainerAttributes(id); err == nil {
		fmt.Printf("%v", attrs)
		// Output: map[coordinateSystem:Pulkovo 1942 / Gauss-Kruger zone 12 path:test2_1188536112.19]
	} else {
		panic(err)
	}
}

func TestContainerAttributes(t *testing.T) {
	db, err := p4db.New(DSN)
	if err != nil {
		panic(err)
	}
	var id int64 = 13
	if attrs, err := db.ContainerAttributes(id); err == nil {
		for aName, attrVal := range attrs {
			fmt.Println(aName, attrVal)
		}
	} else {
		t.Fatal(err)
	}
	// t.Error()
}
