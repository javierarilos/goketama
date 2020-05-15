package main

import (
	"fmt"
	"github.com/javierarilos/goketama/ketama"
)

func main() {
	fmt.Println("hello ketama")
	selector, err := ketama.NewKetamaNodeSelector("localhost:11211", "localhost:11222", "localhost:11233")
	if err != nil {
		panic(err)
	}

	server, err := selector.PickServer("somekey")
	if err != nil {
		panic(err)
	}

	fmt.Println("server picked: ", server.String())

}
