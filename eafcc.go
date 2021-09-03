package main

import (
	"fmt"
	"hash"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/crypto/blake2s"

	"github.com/golang/groupcache/lru"
)

// #cgo amd64 386 CFLAGS: -DX86=1
// #cgo LDFLAGS: -L${SRCDIR} -leafcc
// #include <stdlib.h>
// #include <eafcc.h>
// void update_cb_c(void *user_data);
// void update_cb_go(void *user_data);
// typedef void (*eafcc_update_cb_fn)(void*);
import "C"

type CFGCenter struct {
	sync.RWMutex
	cc       unsafe.Pointer
	// cache    map[string]*CFGValue
	cache *lru.Cache
	cacheSize int
	hashSalt []byte
	updateCB func()
	hasherPool *sync.Pool
}

type CFGContext struct {
	ctx  unsafe.Pointer
	raw string
	hash []byte
	
}

type CFGValue struct {
	ContextType string
	Value       string
}

type eafccInstanceStorageForCGo struct {
	sync.RWMutex
	store map[unsafe.Pointer]*CFGCenter
	idGen uint64
}

func (s *eafccInstanceStorageForCGo) GetNewID() uint64 {
	return atomic.AddUint64(&s.idGen, 1)
}

func (s *eafccInstanceStorageForCGo) Put(p unsafe.Pointer, c *CFGCenter) {
	s.Lock()
	defer s.Unlock()
	s.store[p] = c
}

func (s *eafccInstanceStorageForCGo) Get(p unsafe.Pointer) *CFGCenter {
	s.RLock()
	defer s.RUnlock()
	return s.store[p]
}

var eafccInstanceStorageForCGoInst = eafccInstanceStorageForCGo{store: make(map[unsafe.Pointer]*CFGCenter)}

//export update_cb_go
func update_cb_go(userData unsafe.Pointer) {
	if cc := eafccInstanceStorageForCGoInst.Get(userData); cc != nil {
		newCache := lru.New(cc.cacheSize)
		cc.Lock()
		defer cc.Unlock()
		cc.cache = newCache
		if cc.updateCB != nil {
			cc.updateCB()
		}
		
	}
}

func NewCfgCenter(cfg string, updateCB func(), cacheSize int, cacheSalt []byte) *CFGCenter {
	ccfg := C.CString(cfg)
	defer C.free(unsafe.Pointer(ccfg))

	ret := CFGCenter{
		cache: lru.New(cacheSize),
		hashSalt: cacheSalt,
		cacheSize: cacheSize,
		hasherPool: &sync.Pool{
			New: func()interface{} {
				ret, _ := blake2s.New256(nil)
				// fmt.Println("123123")
				return ret
			},
		},
	}

	if len(cacheSalt) == 0 {
		ret.hashSalt = make([]byte, 8)
		rand.Read(ret.hashSalt)
	}

	punsafe := unsafe.Pointer(uintptr(eafccInstanceStorageForCGoInst.GetNewID()))
	if handler := C.new_config_center_client(
		ccfg,
		(C.eafcc_update_cb_fn)(unsafe.Pointer(C.update_cb_go)),
		punsafe,
	); handler != nil {
		ret.cc = unsafe.Pointer(handler)

		eafccInstanceStorageForCGoInst.Put(punsafe, &ret)
		return &ret
	}
	return nil
}

func (c *CFGCenter)batchReadFromCache (ccCtx *CFGContext, keys []string)(values []*CFGValue, missingKeys []string, missingCacheKeys []string){
	values = make([]*CFGValue, len(keys))
	for idx, key := range keys {
		cacheKey := string(makeCacheKey(c.hasherPool, ccCtx, key))
		if v, ok := c.cache.Get(cacheKey); ok {
			values[idx] = v.(*CFGValue)
		} else {
			if missingKeys == nil {
				missingKeys = make([]string, 0, len(keys))
				missingCacheKeys = make([]string, 0, len(keys))
			}
			missingKeys = append(missingKeys, key)
			missingCacheKeys = append(missingCacheKeys, cacheKey)
		}
	}
	return values, missingKeys, missingCacheKeys
}


func (c *CFGCenter) GetCfg(ccCtx *CFGContext, keys []string) []*CFGValue {

	c.RWMutex.RLock()
	values, missingKeys, missingCacheKeys := c.batchReadFromCache(ccCtx, keys)
	c.RWMutex.RUnlock()

	if missingKeys == nil {
		return values
	}


	t := c.GetCfgRawNoCache(ccCtx, missingKeys)

	c.RWMutex.Lock()
	for idx, value := range t {
		c.cache.Add(missingCacheKeys[idx], &value)
	}
	c.RWMutex.Unlock()

	// reload the full key list in case of update callback, we have to ensure that the returned value is from the same config version
	c.RWMutex.RLock()
	values, missingKeys, missingCacheKeys = c.batchReadFromCache(ccCtx, keys)
	c.RWMutex.RUnlock()
	

	return values
}

func makeCacheKey(pool *sync.Pool, ccCtx *CFGContext, key string) []byte {
	hasher := pool.Get().(hash.Hash)
	hasher.Write(ccCtx.hash)
	hasher.Write(toBytes(key))
	t := hasher.Sum(nil)
	hasher.Reset()
	pool.Put(hasher)
	return t
}

func (c *CFGCenter) GetCfgRawNoCache(ccCtx *CFGContext, keys []string) []CFGValue {
	if len(keys) == 0 {
		return nil
	}

	ckeys := make([]unsafe.Pointer, 0, len(keys))
	for _, key := range keys {
		ckey := C.CString(key)
		ckeys = append(ckeys, unsafe.Pointer(ckey))
		defer C.free(unsafe.Pointer(ckey))
	}

	if ccCtx.ctx == nil {
		cctx := C.CString(ccCtx.raw)
		defer C.free(unsafe.Pointer(cctx))
		if handler := C.new_context(cctx); handler != nil {
			ccCtx.ctx = unsafe.Pointer(handler)
		} else {
			return nil
		}
	}
	

	t := C.get_config((*C.eafcc_CFGCenter)(c.cc), (*C.eafcc_Context)(ccCtx.ctx), (**C.char)(unsafe.Pointer(&ckeys[0])), C.ulong(len(keys)))

	ret := make([]CFGValue, 0, len(keys))

	for i := 0; i < len(keys); i++ {
		tmpP := unsafe.Pointer(uintptr(unsafe.Pointer(t)) + uintptr(i)*unsafe.Sizeof(C.eafcc_ConfigValue{}))
		t := (*C.eafcc_ConfigValue)(tmpP)
		contextType := C.GoString(t.content_type)
		value := C.GoString(t.value)
		ret = append(ret, CFGValue{contextType, value})
	}
	C.free_config_value(t, C.ulong(len(keys)))
	return ret
}

func (c *CFGCenter) NewContext(ccCtx string) *CFGContext {

	hasher := c.hasherPool.Get().(hash.Hash)
	hasher.Write(c.hashSalt)
	hasher.Write(toBytes(ccCtx))
	hashVal := hasher.Sum(nil)
	hasher.Reset()
	c.hasherPool.Put(hasher)
	ret := &CFGContext{hash: hashVal, raw: ccCtx}

	// wo don't need to call into C library now, we want to check cache first
	return ret
}

func (c *CFGContext) Free() {
	if c.ctx != nil {
		C.free_context((*C.eafcc_Context)(c.ctx))
	}
	
}


func toString(bytes []byte) string {
	hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&bytes))
	return *(*string)(unsafe.Pointer(&reflect.StringHeader{
		Data: hdr.Data,
		Len:  hdr.Len,
	}))
}

func toBytes(str string) []byte {
	hdr := *(*reflect.StringHeader)(unsafe.Pointer(&str))
	return *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: hdr.Data,
		Len:  hdr.Len,
		Cap:  hdr.Len,
	}))
}


func test_raw_get() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	dir, _ := os.Getwd()
	fmt.Println(dir)
	cc := NewCfgCenter(`{
		"storage_backend": {
			"type": "filesystem",
			"path": "../../test/mock_data/filesystem_backend/"
		}
	}`, nil, 1024*1024*1024, nil)

	wg := sync.WaitGroup{}
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()

			for x := 0; x < 6000000; x++ {
				ctx := cc.NewContext("foo=123\nbar=456")

				values := cc.GetCfgRawNoCache(ctx, []string{"my_key", "my_key", "my_key"})
				ctx.Free()

				contextType, value := values[0].ContextType, values[0].Value
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


func test_cache_get() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	dir, _ := os.Getwd()
	fmt.Println(dir)
	cc := NewCfgCenter(`{
		"storage_backend": {
			"type": "filesystem",
			"path": "../../test/mock_data/filesystem_backend/"
		}
	}`, nil, 1024*1024*1024, nil)

	wg := sync.WaitGroup{}
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()

			for x := 0; x < 60000000; x++ {
				ctx := cc.NewContext("foo=123\nbar=456")

				values := cc.GetCfg(ctx, []string{"my_key", "my_key", "my_key"})
				ctx.Free()

				contextType, value := values[0].ContextType, values[0].Value
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

func main() {
	// test_raw_get()
	test_cache_get()
}