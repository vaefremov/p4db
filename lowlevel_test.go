package p4db_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/vaefremov/p4db"
)

func TestLogRecords(t *testing.T) {
	db := p4db.MustNew(DSN)
	if records, err := db.LogRecords(15130268, "Containers"); err == nil {
		if records[0].Operation != "Create" {
			t.Error("Create expected")
		}
		if records[6].Operation != "DeleteTree" {
			t.Error("Create expected")
		}
		if expectedTime, err := time.Parse(time.RFC3339, "2018-10-12T15:56:50Z"); err == nil {
			tDiff := expectedTime.Sub(records[6].Modified)
			if tDiff.Seconds() != 0 {
				t.Error("Non-zero time difference", tDiff)
			}
		} else {
			t.Error(err.Error())
		}
	} else {
		t.Error(err.Error())
	}

	// t.Error("Be happy")
}

func ExampleLogRecords() {
	db := p4db.MustNew(DSN)
	if records, err := db.LogRecords(15130268, "Containers"); err == nil {
		for i, r := range records {
			fmt.Println(i, r)
			// Output
		}
	}
	if records, err := db.LogRecords(47633893, "DataValuesC"); err == nil {
		for i, r := range records {
			fmt.Println(i, r)
			// Output
		}
	}
}
