package eafcc

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
	"time"
	"unsafe"

	"golang.org/x/crypto/blake2s"

	"github.com/golang/groupcache/lru"
)

// #cgo amd64 386 CFLAGS: -DX86=1
// #cgo LDFLAGS: -L${SRCDIR} -leafcc
// #include <stdint.h>
// #include <stdlib.h>
// #include <stdbool.h>
// #include <eafcc.h>
// void update_cb_go(void *update_info ,void *user_data);
// typedef void (*eafcc_update_cb_fn)(void*, void*);
import "C"

type Differ struct{
	ptr unsafe.Pointer
}

type CFGCenter struct {
	sync.RWMutex
	cc unsafe.Pointer
}

type Namespace struct {
	sync.RWMutex
	cc unsafe.Pointer
	cache      *lru.Cache
	cacheSize  int
	hashSalt   []byte
	updateCB   func(*Namespace ,Differ)
	hasherPool *sync.Pool
}

type WhoAmI struct {
	ctx  unsafe.Pointer
	raw  string
	hash []byte
}

type CFGValue struct {
	Key         string
	ContextType string
	Value       string
	Reason *CFGValueReason
}

type CFGValueReason struct {
	Pri float32
	IsNeg bool
	LinkPath string
	RulePath string
	ResPath string
}

type namespaceInstanceStorageForCGo struct {
	sync.RWMutex
	store map[unsafe.Pointer]*Namespace
	idGen uint64
}

type CFGViewMode int

const (
	CFGViewModeOverlaidView     CFGViewMode = 0
	CFGViewModeAllLinkedResView CFGViewMode = 1
)

type NotifyLevel uint32 

const (
	NotifyLevelNoNotify = 0
	NotifyLevelNotifyWithoutChangedKeysByGlobal = 1
	NotifyLevelNotifyWithoutChangedKeysInNamespace = 2
	NotifyLevelNotifyWithMaybeChangedKeys = 3
)

func (s *namespaceInstanceStorageForCGo) GetNewID() uint64 {
	return atomic.AddUint64(&s.idGen, 1)
}

func (s *namespaceInstanceStorageForCGo) Put(p unsafe.Pointer, c *Namespace) {
	s.Lock()
	defer s.Unlock()
	s.store[p] = c
}

func (s *namespaceInstanceStorageForCGo) Get(p unsafe.Pointer) *Namespace {
	s.RLock()
	defer s.RUnlock()
	return s.store[p]
}

var namespaceInstanceStorageForCGoInst = namespaceInstanceStorageForCGo{store: make(map[unsafe.Pointer]*Namespace)}

// be careful, there must not have a space between `//`` and `export`
//export update_cb_go
func update_cb_go(differ unsafe.Pointer, userData unsafe.Pointer) {
	if ns := namespaceInstanceStorageForCGoInst.Get(userData); ns != nil {
		newCache := lru.New(ns.cacheSize)
		ns.Lock()
		defer ns.Unlock()
		ns.cache = newCache
		if ns.updateCB != nil {
			ns.updateCB(ns, Differ{ptr:differ})
		}

	}
}

func NewCfgCenter(cfg string) *CFGCenter {
	ccfg := C.CString(cfg)
	defer C.free(unsafe.Pointer(ccfg))

	ret := CFGCenter{}

	if handler := C.new_config_center_client(
		ccfg,
	); handler != nil {
		ret.cc = unsafe.Pointer(handler)
		return &ret
	}
	return nil
}

func (cc *CFGCenter) CreateNamespace(namespace string, notifyLevel NotifyLevel, updateCB func(*Namespace, Differ), cacheSize int, cacheSalt []byte) *Namespace {
	cnamespace := C.CString(namespace)
	defer C.free(unsafe.Pointer(cnamespace))

	ret := Namespace{
		cache:     lru.New(cacheSize),
		hashSalt:  cacheSalt,
		cacheSize: cacheSize,
		hasherPool: &sync.Pool{
			New: func() interface{} {
				ret, _ := blake2s.New256(nil)
				return ret
			},
		},
		updateCB: updateCB,
	}

	if len(cacheSalt) == 0 {
		ret.hashSalt = make([]byte, 8)
		rand.Read(ret.hashSalt)
	}

	userdata := unsafe.Pointer(uintptr(namespaceInstanceStorageForCGoInst.GetNewID()))
	if handler := C.create_namespace(
		(*C.eafcc_CFGCenter)(cc.cc),
		cnamespace,
		C.eafcc_UpdateNotifyLevel(notifyLevel),
		(C.eafcc_update_cb_fn)(unsafe.Pointer(C.update_cb_go)),
		userdata,
	); handler != nil {
		ret.cc = unsafe.Pointer(handler)

		namespaceInstanceStorageForCGoInst.Put(userdata, &ret)
		return &ret
	}
	return nil
}

func (c *Namespace) batchReadFromCache(whoami *WhoAmI, keys []string, viewMode CFGViewMode, needExplain bool) (values map[string][]*CFGValue, missingKeys []string, missingCacheKeysMap map[string]string) {
	values = make(map[string][]*CFGValue, len(keys))
	for _, key := range keys {
		cacheKey := string(makeCacheKey(c.hasherPool, whoami, key, viewMode, needExplain))
		if v, ok := c.cache.Get(cacheKey); ok {
			values[key] = v.([]*CFGValue)
		} else {
			if missingKeys == nil {
				missingKeys = make([]string, 0, len(keys))
				missingCacheKeysMap = make(map[string]string, len(keys))
			}
			missingKeys = append(missingKeys, key)
			missingCacheKeysMap[key] = cacheKey
		}
	}
	return values, missingKeys, missingCacheKeysMap
}

func (c *Namespace) GetCfg(whoami *WhoAmI, keys []string, viewMode CFGViewMode, needExplain bool) (map[string][]*CFGValue, error) {

	c.RWMutex.RLock()
	values, missingKeys, missingCacheKeysMap := c.batchReadFromCache(whoami, keys, viewMode, needExplain)
	c.RWMutex.RUnlock()

	if missingKeys == nil {
		return values, nil
	}

	t, err := c.GetCfgRawNoCache(whoami, missingKeys, viewMode, needExplain)
	if err != nil {
		return nil, err
	}

	c.RWMutex.Lock()
	for key, value := range t {
		c.cache.Add(missingCacheKeysMap[key], value)
	}
	c.RWMutex.Unlock()

	// reload the full key list in case of update callback, we have to ensure that the returned value is from the same config version
	c.RWMutex.RLock()
	values, missingKeys, missingCacheKeysMap = c.batchReadFromCache(whoami, keys, viewMode, needExplain)
	c.RWMutex.RUnlock()

	return values, nil
}

func makeCacheKey(pool *sync.Pool, whoami *WhoAmI, key string, viewMode CFGViewMode, needExplain bool) []byte {
	hasher := pool.Get().(hash.Hash)
	hasher.Write(whoami.hash)
	hasher.Write([]byte{'|'})
	hasher.Write(toBytes(key))
	hasher.Write([]byte{'|'})
	hasher.Write([]byte{byte(viewMode)})
	if needExplain {
		hasher.Write([]byte{byte(1)})
	} else {
		hasher.Write([]byte{byte(0)})
	}

	t := hasher.Sum(nil)
	hasher.Reset()
	pool.Put(hasher)
	return t
}

func (c *Namespace) GetCfgRawNoCache(whoami *WhoAmI, keys []string, viewMode CFGViewMode, needExplain bool) (map[string][]*CFGValue, error) {
	
	ckeys , cViewMode , cNeedExplain, err := convertGetCFGInput(whoami, keys, viewMode, needExplain)
	if err != nil {
		return nil, err
	}

	t := C.get_config((*C.eafcc_NamespaceScopedCFGCenter)(c.cc), (*C.eafcc_WhoAmI)(whoami.ctx), (**C.char)(unsafe.Pointer(&ckeys[0])), C.ulong(len(keys)), cViewMode, C.uchar(cNeedExplain))
	for _, ckey := range ckeys {
		C.free(unsafe.Pointer(ckey))
	}

	return convertGetCFGOutput(t)
}

func (c *Namespace) NewWhoAmI(whoAmI string) *WhoAmI {

	hasher := c.hasherPool.Get().(hash.Hash)
	hasher.Write(c.hashSalt)
	hasher.Write(toBytes(whoAmI))
	hashVal := hasher.Sum(nil)
	hasher.Reset()
	c.hasherPool.Put(hasher)
	ret := &WhoAmI{hash: hashVal, raw: whoAmI}

	// wo don't need to call into C library now, we want to check cache first
	return ret
}

func (c *WhoAmI) Free() {
	if c.ctx != nil {
		C.free_context((*C.eafcc_WhoAmI)(c.ctx))
	}
}



func convertGetCFGInput(whoami *WhoAmI, keys []string, viewMode CFGViewMode, needExplain bool) (ckeys []unsafe.Pointer, cViewMode C.eafcc_ViewMode, cNeedExplain uint8, err error) {
	if len(keys) == 0 {
		return nil, 0, 0,fmt.Errorf("no input keys")
	}

	ckeys = make([]unsafe.Pointer, 0, len(keys))
	for _, key := range keys {
		ckey := C.CString(key)
		ckeys = append(ckeys, unsafe.Pointer(ckey))
		// defer C.free(unsafe.Pointer(ckey))
	}

	if whoami.ctx == nil {
		cctx := C.CString(whoami.raw)
		defer C.free(unsafe.Pointer(cctx))
		if handler := C.new_whoami(cctx); handler != nil {
			whoami.ctx = unsafe.Pointer(handler)
		} else {
			return nil, 0, 0, fmt.Errorf("create whoami in C library got error")
		}
	}
 
	cViewMode = C.eafcc_ViewMode(viewMode)
	cNeedExplain = 0
	if needExplain {
		cNeedExplain = 1
	}
	return
}

func convertGetCFGOutput(cValues *C.eafcc_ConfigValues) (map[string][]*CFGValue, error) {

	valueCnt := int(cValues.len)
	t := cValues.ptr

	ret := make(map[string][]*CFGValue, valueCnt)

	for i := 0; i < valueCnt; i++ {
		tmpP := unsafe.Pointer(uintptr(unsafe.Pointer(t)) + uintptr(i)*unsafe.Sizeof(C.eafcc_ConfigValue{}))
		t := (*C.eafcc_ConfigValue)(tmpP)
		key := C.GoString(t.key)
		contextType := C.GoString(t.content_type)
		value := C.GoString(t.value)

		var reason *CFGValueReason = nil
		if t.reason != nil {
			r := (*C.eafcc_ConfigValueReason)(t.reason)
			reason = &CFGValueReason{
				Pri: float32(r.pri),
				IsNeg: bool(r.is_neg),
				RulePath: C.GoString(r.rule_path),
				LinkPath: C.GoString(r.link_path),
				ResPath: C.GoString(r.res_path),
			}
		}

		ret[key] = append(ret[key], &CFGValue{key, contextType, value, reason})
	}

	C.free_config_values(cValues)
	return ret, nil
}


func (d *Differ) GetFromOld(whoami *WhoAmI, keys []string, viewMode CFGViewMode, needExplain bool) (map[string][]*CFGValue, error){
	ckeys , cViewMode , cNeedExplain, err := convertGetCFGInput(whoami, keys, viewMode, needExplain)
	if err != nil {
		return nil, err
	}

	t := C.differ_get_from_old((*C.eafcc_Differ)(d.ptr), (*C.eafcc_WhoAmI)(whoami.ctx), (**C.char)(unsafe.Pointer(&ckeys[0])), C.ulong(len(keys)), cViewMode, C.uchar(cNeedExplain))
	for _, ckey := range ckeys {
		C.free(unsafe.Pointer(ckey))
	}
	return convertGetCFGOutput(t)
}

func (d *Differ) GetFromNew(whoami *WhoAmI, keys []string, viewMode CFGViewMode, needExplain bool) (map[string][]*CFGValue, error){
	ckeys , cViewMode , cNeedExplain, err := convertGetCFGInput(whoami, keys, viewMode, needExplain)
	if err != nil {
		return nil, err
	}

	t := C.differ_get_from_new((*C.eafcc_Differ)(d.ptr), (*C.eafcc_WhoAmI)(whoami.ctx), (**C.char)(unsafe.Pointer(&ckeys[0])), C.ulong(len(keys)), cViewMode, C.uchar(cNeedExplain))
	for _, ckey := range ckeys {
		C.free(unsafe.Pointer(ckey))
	}
	return convertGetCFGOutput(t)
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


func test_update_cb(ns *Namespace, d Differ) {
	ctx := ns.NewWhoAmI("foo=123\nbar=456")
	values_new, _ := d.GetFromNew(ctx, []string{"my_key", "my_key", "my_key"}, CFGViewModeOverlaidView, false)
	values_old, _ := d.GetFromOld(ctx, []string{"my_key", "my_key", "my_key"}, CFGViewModeOverlaidView, false)
	ctx.Free()

	fmt.Println("old", values_old["my_key"][0].Value)
	fmt.Println("new", values_new["my_key"][0].Value)
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
	}`)

	ns := cc.CreateNamespace("/", NotifyLevelNotifyWithoutChangedKeysByGlobal, test_update_cb, 1024*1024*1024, nil)
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
	}`)
	ns := cc.CreateNamespace("/", NotifyLevelNotifyWithoutChangedKeysByGlobal, nil, 1024*1024*1024, nil)
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

func main() {
	test_raw_get()
	// test_cache_get()
}
