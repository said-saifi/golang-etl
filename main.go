package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

func main() {
	// runtime.GOMAXPROCS(1) Uncomment this in case you want test with one processor
	start := time.Now()

	c := make(chan Order)
	// done := make(chan bool)  load func does not have to run in a separate goroutine

	go transform(c)
	load(c)

	// <-done
	fmt.Println(time.Since(start))
}

// Product holds details of a product
type Product struct {
	PartNumber string
	UnitCost   float64
	UnitPrice  float64
}

// Order holds details of a customer order
type Order struct {
	CustomerNumber int
	PartNumber     string
	Quantity       int

	UnitCost  float64
	UnitPrice float64
}

func transform(c chan Order) {
	pf, _ := os.Open("./productList.txt")
	defer pf.Close()

	pr := csv.NewReader(pf)
	prs, _ := pr.ReadAll()
	productList := make(map[string]*Product)

	for _, r := range prs {
		p := new(Product)
		p.PartNumber = r[0]
		p.UnitCost, _ = strconv.ParseFloat(r[1], 64)
		p.UnitPrice, _ = strconv.ParseFloat(r[2], 64)
		productList[p.PartNumber] = p
	}

	var wg sync.WaitGroup

	of, _ := os.Open("./orders.txt")
	defer of.Close()

	or := csv.NewReader(of)
	for record, err := or.Read(); err == nil; record, err = or.Read() {
		wg.Add(1)

		go func(record []string){
			time.Sleep(3 * time.Millisecond)

			o := new(Order)
			o.CustomerNumber, _ = strconv.Atoi(record[0])
			o.PartNumber = record[1]
			o.Quantity, _ = strconv.Atoi(record[2])
			o.UnitCost = productList[o.PartNumber].UnitCost
			o.UnitPrice = productList[o.PartNumber].UnitPrice

			c <- *o
			defer wg.Done()
		}(record)

	}
	wg.Wait()
	close(c)

}

func load(c chan Order) {
	f, _ := os.Create("./dest.txt")
	defer f.Close()

	fmt.Fprintf(f, "%20s%16s%13s%13s%16s%16s", "Part Number", "Quantity",
		"Unit Cost", "Unit Price", "Total Cost", "Total Price\n")

	var wg sync.WaitGroup // Used just to not return before end of last row
	for o := range c {
		wg.Add(1)
		go func(o Order) {
			time.Sleep(1 * time.Millisecond)
			fmt.Fprintf(f, "%20s %15d %12.2f %12.2f %15.2f%15.2f\n",
				o.PartNumber, o.Quantity, o.UnitCost, o.UnitPrice,
				o.UnitCost*float64(o.Quantity), o.UnitPrice*float64(o.UnitPrice))
			defer wg.Done()
		}(o)
	}

	wg.Wait()



}
