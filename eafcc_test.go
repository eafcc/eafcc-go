package eafcc

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"
)

func test_update_cb(ns *Namespace, d Differ) {
	ctx := ns.NewWhoAmI("foo=123\nbar=456")
	values_new, _ := d.GetFromNew(ctx, []string{"my_key", "my_key", "my_key"}, CFGViewModeOverlaidView, false)
	values_old, _ := d.GetFromOld(ctx, []string{"my_key", "my_key", "my_key"}, CFGViewModeOverlaidView, false)
	ctx.Free()

	fmt.Println("old", values_old["my_key"][0].Value)
	fmt.Println("new", values_new["my_key"][0].Value)
}


func TestRawGet(t *testing.T) {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	dir, _ := os.Getwd()
	fmt.Println(dir)
	cc, err := NewCfgCenter(`{
		"storage_backend": {
			"type": "filesystem",
			"path": "../../test/mock_data/filesystem_backend/"
		}
	}`)
	if err != nil {
		t.Fatal(err)
	}

	ns,_ := cc.CreateNamespace("/", NotifyLevelNotifyWithoutChangedKeysByGlobal, test_update_cb, 1024*1024*1024, nil)
	wg := sync.WaitGroup{}
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()

			for x := 0; x < 60000; x++ {
				ctx := ns.NewWhoAmI("foo=123\nbar=456")

				values, err := ns.GetCfgRawNoCache(ctx, []string{"my_key", "my_key", "my_key"}, CFGViewModeOverlaidView, false)
				ctx.Free()
				if err != nil {
					panic(err)
				}

				contextType, value := values["my_key"][0].ContextType, values["my_key"][0].Value
				if contextType != "application/json" {
					panic(contextType)
				}
				_ = value
				// if value != `{"aaa":[{},{"bbb":"hahaha"}]}` {
				// 	panic(contextType)
				// }
				time.Sleep(1 * time.Second)
			}
		}()
	}

	wg.Wait()
	print("=fin")
	// runtime.GC()
	// time.Sleep(1000 * time.Hour)

}

func TestCacheGet(t *testing.T) {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	dir, _ := os.Getwd()
	fmt.Println(dir)
	cc, err := NewCfgCenter(`{
		"storage_backend": {
			"type": "filesystem",
			"path": "../../test/mock_data/filesystem_backend/"
		}
	}`)
	if err != nil {
		t.Fatal(err)
	}

	ns, _ := cc.CreateNamespace("/", NotifyLevelNotifyWithoutChangedKeysByGlobal, nil, 1024*1024*1024, nil)
	wg := sync.WaitGroup{}
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()

			for x := 0; x < 60000000; x++ {
				ctx := ns.NewWhoAmI("foo=123\nbar=456")

				values, err := ns.GetCfg(ctx, []string{"my_key", "my_key", "my_key"}, CFGViewModeOverlaidView, false)
				ctx.Free()
				if err != nil {
					panic(err)
				}

				contextType, value := values["my_key"][0].ContextType, values["my_key"][0].Value
				if contextType != "application/json" {
					panic(contextType)
				}
				if value != `{"aaa":[{},{"bbb":"hahaha"}]}` {
					panic(contextType)
				}
			}
		}()
	}

	wg.Wait()
	print("=fin")
	// runtime.GC()
	// time.Sleep(1000 * time.Hour)
}

