package server

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	rerrors "github.com/smallnest/rpcx/errors"
	"github.com/smallnest/rpcx/log"
)

// Precompute the reflect type for error. Can't use error directly
// because Typeof takes an empty interface value. This is annoying.
// 错误类型(通过nil来进行反射其对应的Error)
var typeOfError = reflect.TypeOf((*error)(nil)).Elem()

// Context类型(通过nil来进行反射其对应的Context)
// Precompute the reflect type for context.
var typeOfContext = reflect.TypeOf((*context.Context)(nil)).Elem()

// 方法类型：包括方法属性、参数属性、响应属性
type methodType struct {
	sync.Mutex // protects counters
	method     reflect.Method
	ArgType    reflect.Type
	ReplyType  reflect.Type
	// numCalls   uint
}

// 函数类型：包括函数属性、参数属性、返回内容属性
type functionType struct {
	sync.Mutex // protects counters
	fn         reflect.Value
	ArgType    reflect.Type
	ReplyType  reflect.Type
}

// rpc提供的service
// 包括service名、服务对应的接收者、接收者类型、service注册的方法以及注册的函数
type service struct {
	name     string                   // name of service
	rcvr     reflect.Value            // receiver of methods for the service
	typ      reflect.Type             // type of the receiver
	method   map[string]*methodType   // registered methods
	function map[string]*functionType // registered functions
}

// 判断提供的service内容是否是可导出的 外部使用的
func isExported(name string) bool {
	rune, _ := utf8.DecodeRuneInString(name)  // 获取name的第一个字符 及其对应utf-8编码后的编码字节数
	return unicode.IsUpper(rune)
}

//
func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {  // 类型是否是指针   若是则需要使用Elem()来操作
		t = t.Elem()
	}
	// PkgPath will be non-empty even for an exported type,
	// so we need to check the type name as well.
	return isExported(t.Name()) || t.PkgPath() == ""  // 检查t的name首字母以及PkgPath检查是否能导出或内嵌类型
}

// Register publishes in the server the set of methods of the
// receiver value that satisfy the following conditions:
//	- exported method of exported type
//	- three arguments, the first is of context.Context, both of exported type for three arguments
//	- the third argument is a pointer
//	- one return value, of type error
// It returns an error if the receiver is not an exported type or has
// no suitable methods. It also logs the error.
// The client accesses each method using a string of the form "Type.Method",
// where Type is the receiver's concrete type.
// server端提供的service需要一定的条件方能对外发布
// - 所属的类型属于可导出其对应的method也是可导出的，
// - 对应的method包括三个输入参数： 1、context.Context 2、三个参数也是可导出类型 3、第3个参数是一个pointer类型，仅有一个返回值(类型error)
// 注意：
//   1、对应的方法的receiver不属于可导出的 或 没有对应的方法 都会返回一个error ，并记录操作
//   2、client访问每个method都采用类似"Type.Method"格式的字符串
//   其中"Type.Method"中的Type是一个具体的receiver类型
// Register方法完成两个操作：1、先在本地map 记录新增的service信息
func (s *Server) Register(rcvr interface{}, metadata string) error {
	sname, err := s.register(rcvr, "", false)  // 当未指定service的name同时useName标志=false 则使用rcvr类型的name
	if err != nil {
		return err
	}
	if s.Plugins == nil {  // plugin
		s.Plugins = &pluginContainer{}
	}
	return s.Plugins.DoRegister(sname, rcvr, metadata)  // 回调plugin(使用其他的注册中心等)
}

// RegisterName is like Register but uses the provided name for the type
// instead of the receiver's concrete type.
// 类似Register不过可以指定类型名称 而不是用receiver的类型来填充
func (s *Server) RegisterName(name string, rcvr interface{}, metadata string) error {
	if s.Plugins == nil {
		s.Plugins = &pluginContainer{}
	}

	s.Plugins.DoRegister(name, rcvr, metadata)
	_, err := s.register(rcvr, name, true)
	return err
}

// RegisterFunction publishes a function that satisfy the following conditions:
//	- three arguments, the first is of context.Context, both of exported type for three arguments
//	- the third argument is a pointer
//	- one return value, of type error
// The client accesses function using a string of the form "servicePath.Method".
//
// 用于完成function的注册 而非method，不过需要遵守如下的要求
// - 包括三个输入参数：第一个参数必须是context.Context,参数都是可导出类型
// - 第三个参数是一个pointer指针
// - 返回值包括一个error类型
// 通过使用"servicePath.function"来访问function
//
func (s *Server) RegisterFunction(servicePath string, fn interface{}, metadata string) error {
	fname, err := s.registerFunction(servicePath, fn, "", false)
	if err != nil {
		return err
	}
	if s.Plugins == nil {
		s.Plugins = &pluginContainer{}
	}

	return s.Plugins.DoRegisterFunction(fname, fn, metadata)
}

// RegisterFunctionName is like RegisterFunction but uses the provided name for the function
// instead of the function's concrete type.
//
// 和RegisterFuntion类似 只不过增加了可指定function的name
func (s *Server) RegisterFunctionName(servicePath string, name string, fn interface{}, metadata string) error {
	if s.Plugins == nil {
		s.Plugins = &pluginContainer{}
	}

	s.Plugins.DoRegisterFunction(name, fn, metadata)
	_, err := s.registerFunction(servicePath, fn, name, true)
	return err
}

// 注册service
func (s *Server) register(rcvr interface{}, name string, useName bool) (string, error) {
	s.serviceMapMu.Lock()
	defer s.serviceMapMu.Unlock()
	if s.serviceMap == nil {
		s.serviceMap = make(map[string]*service)
	}

	// 构建service 便于存放到本地map里面
	service := new(service)
	service.typ = reflect.TypeOf(rcvr)
	service.rcvr = reflect.ValueOf(rcvr)
	sname := reflect.Indirect(service.rcvr).Type().Name() // Type
	if useName {
		sname = name
	}
	if sname == "" {
		errorStr := "rpcx.Register: no service name for type " + service.typ.String()
		log.Error(errorStr)
		return sname, errors.New(errorStr)
	}
	if !useName && !isExported(sname) {
		errorStr := "rpcx.Register: type " + sname + " is not exported"
		log.Error(errorStr)
		return sname, errors.New(errorStr)
	}
	service.name = sname

	// Install the methods
	service.method = suitableMethods(service.typ, true) // 获取注册service对应的具体类型中可导出的method

	if len(service.method) == 0 {
		var errorStr string

		// To help the user, see if a pointer receiver would work.
		// 当method的receiver是一个pointer
		method := suitableMethods(reflect.PtrTo(service.typ), false)
		if len(method) != 0 {
			errorStr = "rpcx.Register: type " + sname + " has no exported methods of suitable type (hint: pass a pointer to value of that type)"
		} else {
			errorStr = "rpcx.Register: type " + sname + " has no exported methods of suitable type"
		}
		log.Error(errorStr)
		return sname, errors.New(errorStr)
	}
	s.serviceMap[service.name] = service
	return sname, nil
}

func (s *Server) registerFunction(servicePath string, fn interface{}, name string, useName bool) (string, error) {
	s.serviceMapMu.Lock()
	defer s.serviceMapMu.Unlock()
	if s.serviceMap == nil {
		s.serviceMap = make(map[string]*service)
	}

	ss := s.serviceMap[servicePath]
	if ss == nil {
		ss = new(service)
		ss.name = servicePath
		ss.function = make(map[string]*functionType)
	}

	f, ok := fn.(reflect.Value)
	if !ok {
		f = reflect.ValueOf(fn)
	}
	if f.Kind() != reflect.Func {
		return "", errors.New("function must be func or bound method")
	}

	fname := runtime.FuncForPC(reflect.Indirect(f).Pointer()).Name()
	if fname != "" {
		i := strings.LastIndex(fname, ".")
		if i >= 0 {
			fname = fname[i+1:]
		}
	}
	if useName {
		fname = name
	}
	if fname == "" {
		errorStr := "rpcx.registerFunction: no func name for type " + f.Type().String()
		log.Error(errorStr)
		return fname, errors.New(errorStr)
	}

	t := f.Type()
	if t.NumIn() != 3 {
		return fname, fmt.Errorf("rpcx.registerFunction: has wrong number of ins: %s", f.Type().String())
	}
	if t.NumOut() != 1 {
		return fname, fmt.Errorf("rpcx.registerFunction: has wrong number of outs: %s", f.Type().String())
	}

	// First arg must be context.Context
	ctxType := t.In(0)
	if !ctxType.Implements(typeOfContext) {
		return fname, fmt.Errorf("function %s must use context as  the first parameter", f.Type().String())
	}

	argType := t.In(1)
	if !isExportedOrBuiltinType(argType) {
		return fname, fmt.Errorf("function %s parameter type not exported: %v", f.Type().String(), argType)
	}

	replyType := t.In(2)
	if replyType.Kind() != reflect.Ptr {
		return fname, fmt.Errorf("function %s reply type not a pointer: %s", f.Type().String(), replyType)
	}
	if !isExportedOrBuiltinType(replyType) {
		return fname, fmt.Errorf("function %s reply type not exported: %v", f.Type().String(), replyType)
	}

	// The return type of the method must be error.
	if returnType := t.Out(0); returnType != typeOfError {
		return fname, fmt.Errorf("function %s returns %s, not error", f.Type().String(), returnType.String())
	}

	// Install the methods
	ss.function[fname] = &functionType{fn: f, ArgType: argType, ReplyType: replyType}
	s.serviceMap[servicePath] = ss

	argsReplyPools.Init(argType)
	argsReplyPools.Init(replyType)
	return fname, nil
}

// suitableMethods returns suitable Rpc methods of typ, it will report
// error using log if reportErr is true.
// 获取注册service类型对应的方法：必须是可导出的方法
func suitableMethods(typ reflect.Type, reportErr bool) map[string]*methodType {
	methods := make(map[string]*methodType)
	for m := 0; m < typ.NumMethod(); m++ {  // 遍历typ中包含的method
		method := typ.Method(m)
		mtype := method.Type
		mname := method.Name
		// Method must be exported.
		if method.PkgPath != "" {  // service中需要注册到本地方法必须是可导出的
			continue
		}
		// Method needs four ins: receiver, context.Context, *args, *reply.
		if mtype.NumIn() != 4 { // 确保service提供的方法包括：receiver以及形参有context.Context, *args, *reply构成
			if reportErr {
				log.Info("method", mname, "has wrong number of ins:", mtype.NumIn())
			}
			continue
		}
		// First arg must be context.Context
		ctxType := mtype.In(1)
		if !ctxType.Implements(typeOfContext) {
			if reportErr {
				log.Info("method", mname, " must use context.Context as the first parameter")
			}
			continue
		}

		// Second arg need not be a pointer.
		argType := mtype.In(2)
		if !isExportedOrBuiltinType(argType) {
			if reportErr {
				log.Info(mname, "parameter type not exported:", argType)
			}
			continue
		}
		// Third arg must be a pointer.
		// 第三个参数必须是pointer指针
		replyType := mtype.In(3)
		if replyType.Kind() != reflect.Ptr {
			if reportErr {
				log.Info("method", mname, "reply type not a pointer:", replyType)
			}
			continue
		}
		// Reply type must be exported.
		// Reply类型必须是可导出的
		if !isExportedOrBuiltinType(replyType) {
			if reportErr {
				log.Info("method", mname, "reply type not exported:", replyType)
			}
			continue
		}
		// 输出参数个数=1 并且返回值类型必须是error
		// Method needs one out.
		if mtype.NumOut() != 1 {
			if reportErr {
				log.Info("method", mname, "has wrong number of outs:", mtype.NumOut())
			}
			continue
		}
		// The return type of the method must be error.
		if returnType := mtype.Out(0); returnType != typeOfError {
			if reportErr {
				log.Info("method", mname, "returns", returnType.String(), "not error")
			}
			continue
		}
		methods[mname] = &methodType{method: method, ArgType: argType, ReplyType: replyType}

		argsReplyPools.Init(argType)
		argsReplyPools.Init(replyType)
	}
	return methods
}

// UnregisterAll unregisters all services.
// You can call this method when you want to shutdown/upgrade this node.
//
// 取消注册的services
// 当前进行shutdown或升级节点 可以通过调用该方法
func (s *Server) UnregisterAll() error {
	if s.Plugins == nil {
		s.Plugins = &pluginContainer{}
	}

	var es []error
	for k := range s.serviceMap {
		err := s.Plugins.DoUnregister(k)
		if err != nil {
			es = append(es, err)
		}
	}

	if len(es) > 0 {
		return rerrors.NewMultiError(es)
	}
	return nil
}

// service调用: method
func (s *service) call(ctx context.Context, mtype *methodType, argv, replyv reflect.Value) (err error) {
	// 当调用service进行error恢复  再进行相关的操作：比如对error的处理
	defer func() {
		if r := recover(); r != nil {
			//log.Errorf("failed to invoke service: %v, stacks: %s", r, string(debug.Stack()))
			err = fmt.Errorf("[service internal error]: %v, stacks: %s", r, string(debug.Stack()))
			log.Handle(err)
		}
	}()

	function := mtype.method.Func
	// Invoke the method, providing a new value for the reply.
	// 调用method
	returnValues := function.Call([]reflect.Value{s.rcvr, reflect.ValueOf(ctx), argv, replyv})
	// The return value for the method is an error.
	// 判断返回值类型是否error
	errInter := returnValues[0].Interface()
	if errInter != nil {  // 执行service调用出现了error
		return errInter.(error)
	}

	return nil
}

// service调用：functioin；与method相比缺少了receiver
func (s *service) callForFunction(ctx context.Context, ft *functionType, argv, replyv reflect.Value) (err error) {
	defer func() { // 进行error恢复操作
		if r := recover(); r != nil {
			//log.Errorf("failed to invoke service: %v, stacks: %s", r, string(debug.Stack()))
			err = fmt.Errorf("[service internal error]: %v, stacks: %s", r, string(debug.Stack()))
			log.Handle(err)
		}
	}()

	// Invoke the function, providing a new value for the reply.
	// 调用function
	returnValues := ft.fn.Call([]reflect.Value{reflect.ValueOf(ctx), argv, replyv})
	// The return value for the method is an error.
	// 返回值类型是error
	errInter := returnValues[0].Interface()
	if errInter != nil {
		return errInter.(error)
	}

	return nil
}
