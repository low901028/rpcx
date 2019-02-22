package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/smallnest/rpcx/util"
)

var (
	// Compressors are compressors supported by rpcx. You can add customized compressor in Compressors.
	// RPCX提供的请求压缩类型：None(未压缩)、Gzip(一般请求大于1KB，且指定对应的压缩类型)
	Compressors = map[CompressType]Compressor{
		None: &RawDataCompressor{},
		Gzip: &GzipCompressor{},
	}
)

// MaxMessageLength is the max length of a message.
// Default is 0 that means does not limit length of messages.
// It is used to validate when read messages from io.Reader.
// 用于执行请求消息的大小，默认=0 表示不限制，使用时需要根据实际情况进行设定同时结合对应的压缩类型 提供系统性能(及时性、吞吐量)
var MaxMessageLength = 0

//
// 	 ______________________________________________________________________________消息头Header______________________________________________________________________________________________________________________
// 	|_信息开始0x08_长度=1byte_|_消息版本_长度=1byte_|_消息类型_长度=1bit_|_是否心跳信息_长度=1bit_|_是否单向信息_长度=1bit_|_压缩类型_长度=3bits_|_消息响应类型_长度=2bits_|_编码类型_长度=4bits_|_保留位_长度=4bits_|
//   ______消息ID_____________
//  |_消息64bitsID_长度=8bytes|
//   ______消息长度_______
//  |_消息大小_长度=4bytes|
//   ___________________服务__________________
//  |_服务名_长度=4bytes|_服务内容_长度=nbytes|
//   ___________________服务函数______________
//  |_函数名_长度=4bytes|_函数内容_长度=nbytes|
//   _________________消息元数据________________
//  |_元数据_长度=4bytes|_元数据内容_长度=nbytes|
//   ___________________消息体______________________
//  |_消息体大小_长度=4bytes|_消息体内容_长度=nbytes|
//
// 一个消息由下面的项组成:
//
// Header: 4 字节
// Message ID: 8 字节
// total size: 4 字节, 不包含header和它本身, uint32类型
// servicePath值的长度: 4 字节, uint32类型
// servicePath的值: UTF-8 字符串
// serviceMethod值的长度: 4 字节, uint32类型
// serviceMethod的值: UTF-8 字符串
// metadata的大小: 4 字节, uint32类型
// metadata: 格式: size key1 string size value1 string, 可以包含多个
// playload的大小: 4 字节, uint32类型
// playload的值: slice of byte
const (
	magicNumber byte = 0x08 // 魔数 用于定义rpcx请求的 消息开始
)

var (
	// ErrMetaKVMissing some keys or values are mssing.
	ErrMetaKVMissing = errors.New("wrong metadata lines. some keys or values are missing") // 请求消息中MetaData中key-value丢失
	// ErrMessageTooLong message is too long
	ErrMessageTooLong = errors.New("message is too long") // 消息太长

	ErrUnsupportedCompressor = errors.New("unsupported compressor") // 不支持的压缩类型
)

const (
	// ServiceError contains error info of service invocation
	ServiceError = "__rpcx_error__" // 服务调用的错误 一般到存到metadata或context
)

// MessageType is message type of requests and resposnes.
// 消息类型(请求和响应两种)
type MessageType byte

const (
	// Request is message type of request
	Request MessageType = iota // 请求
	// Response is message type of response
	Response // 响应
)

// MessageStatusType is status of messages.
type MessageStatusType byte // 响应消息状态：正常或错误

const (
	// Normal is normal requests and responses.
	Normal MessageStatusType = iota
	// Error indicates some errors occur.
	Error
)

// CompressType defines decompression type.
type CompressType byte // 压缩类型(原始或GZIP)

const (
	// None does not compress.
	None CompressType = iota
	// Gzip uses gzip compression.
	Gzip
)

// SerializeType defines serialization type of payload.
type SerializeType byte //消息体 编解码类型

const (
	// SerializeNone uses raw []byte and don't serialize/deserialize 直接使用[]byte
	SerializeNone SerializeType = iota
	// JSON for payload.
	JSON
	// ProtoBuffer for payload.
	ProtoBuffer
	// MsgPack for payload
	MsgPack
	// Thrift
	// Thrift for payload
	Thrift
)

// Message is the generic type of Request and Response.
// 消息
type Message struct {
	*Header                         // 消息头 = 4bytes
	ServicePath   string            // 服务名
	ServiceMethod string            // 服务方法
	Metadata      map[string]string // 元数据
	Payload       []byte            // 消息体
	data          []byte            //
}

// NewMessage creates an empty message.
// 新建消息：对应的消息对象属性需要自行设置 添加
func NewMessage() *Message {
	header := Header([12]byte{})
	header[0] = magicNumber // 消息开始默认指定

	return &Message{
		Header: &header,
	}
}

// Header is the first part of Message and has fixed size.
// Format:
//
type Header [12]byte

// CheckMagicNumber checks whether header starts rpcx magic number.
func (h Header) CheckMagicNumber() bool { // 消息开始部分 便于隔离不同的请求消息
	return h[0] == magicNumber
}

// Version returns version of rpcx protocol.
func (h Header) Version() byte { // 消息版本
	return h[1]
}

// SetVersion sets version for this header.
func (h *Header) SetVersion(v byte) { // 设置消息版本
	h[1] = v
}

// MessageType returns the message type.
func (h Header) MessageType() MessageType { // 消息类型
	return MessageType(h[2]&0x80) >> 7
}

// SetMessageType sets message type.
func (h *Header) SetMessageType(mt MessageType) {
	h[2] = h[2] | (byte(mt) << 7)
}

// IsHeartbeat returns whether the message is heartbeat message.
func (h Header) IsHeartbeat() bool { // 心跳
	return h[2]&0x40 == 0x40
}

// SetHeartbeat sets the heartbeat flag.
func (h *Header) SetHeartbeat(hb bool) {
	if hb {
		h[2] = h[2] | 0x40
	} else {
		h[2] = h[2] &^ 0x40
	}
}

// IsOneway returns whether the message is one-way message.
// If true, server won't send responses.
func (h Header) IsOneway() bool { // 是否忽略响应
	return h[2]&0x20 == 0x20
}

// SetOneway sets the oneway flag.
func (h *Header) SetOneway(oneway bool) {
	if oneway {
		h[2] = h[2] | 0x20
	} else {
		h[2] = h[2] &^ 0x20
	}
}

// CompressType returns compression type of messages.
func (h Header) CompressType() CompressType { // 压缩类型
	return CompressType((h[2] & 0x1C) >> 2)
}

// SetCompressType sets the compression type.
func (h *Header) SetCompressType(ct CompressType) {
	h[2] = (h[2] &^ 0x1C) | ((byte(ct) << 2) & 0x1C)
}

// MessageStatusType returns the message status type.
func (h Header) MessageStatusType() MessageStatusType { // 响应状态类型
	return MessageStatusType(h[2] & 0x03)
}

// SetMessageStatusType sets message status type.
func (h *Header) SetMessageStatusType(mt MessageStatusType) {
	h[2] = (h[2] &^ 0x03) | (byte(mt) & 0x03)
}

// SerializeType returns serialization type of payload.
func (h Header) SerializeType() SerializeType { // 编解码类型
	return SerializeType((h[3] & 0xF0) >> 4)
}

// SetSerializeType sets the serialization type.
func (h *Header) SetSerializeType(st SerializeType) {
	h[3] = (h[3] &^ 0xF0) | (byte(st) << 4)
}

// Seq returns sequence number of messages.
func (h Header) Seq() uint64 { // 消息ID
	return binary.BigEndian.Uint64(h[4:])
}

// SetSeq sets  sequence number.
func (h *Header) SetSeq(seq uint64) {
	binary.BigEndian.PutUint64(h[4:], seq)
}

// Clone clones from an message.
func (m Message) Clone() *Message { // 复制消息
	header := *m.Header
	c := GetPooledMsg()
	header.SetCompressType(None) // 压缩类型变为None
	c.Header = &header
	c.ServicePath = m.ServicePath
	c.ServiceMethod = m.ServiceMethod
	return c
}

// Encode encodes messages.
func (m Message) Encode() []byte { // 编码消息
	meta := encodeMetadata(m.Metadata) // 编码metadata将key-value转为key=value&key=value形式

	spL := len(m.ServicePath)   // 服务长度
	smL := len(m.ServiceMethod) // 服务函数

	var err error
	payload := m.Payload          // 消息体
	if m.CompressType() != None { // 压缩
		compressor := Compressors[m.CompressType()]
		if compressor == nil { // 默认使用None压缩类型
			m.SetCompressType(None)
		} else {
			payload, err = compressor.Zip(m.Payload) // GZIP压缩
			if err != nil {                          // 压缩失败
				m.SetCompressType(None)
				payload = m.Payload
			}
		}
	}

	// message = header + ID + total size +
	// servicePath(size(servicePath) 、len(servicePath)) +   // 服务名及内容
	// serviceMethod(size(serviceMethod) 、 len(serviceMethod)) +  // 服务函数及内容
	// metadata(size(metadata) 、len(metadata)) +  // 元数据及内容
	// payload(size(payload) 、 len(payload)) // 消息体及内容

	totalL := (4 + spL) + (4 + smL) + (4 + len(meta)) + (4 + len(payload)) // 消息长度 = size(servicePath) + len(servicePath) + size(serviceMethod) + len(serviceMethod) + size(metadata) + len(metadata) + size(payload) + len(payload)

	// header + dataLen + spLen + sp + smLen + sm + metaL + meta + payloadLen + payload
	metaStart := 12 + 4 + (4 + spL) + (4 + smL) // meata开始位置

	payLoadStart := metaStart + (4 + len(meta)) // payLoad开始位置
	l := 12 + 4 + totalL

	data := make([]byte, l)
	copy(data, m.Header[:]) // 拷贝header内容

	//totalLen
	binary.BigEndian.PutUint32(data[12:16], uint32(totalL))

	binary.BigEndian.PutUint32(data[16:20], uint32(spL))
	copy(data[20:20+spL], util.StringToSliceByte(m.ServicePath))

	binary.BigEndian.PutUint32(data[20+spL:24+spL], uint32(smL))
	copy(data[24+spL:metaStart], util.StringToSliceByte(m.ServiceMethod))

	binary.BigEndian.PutUint32(data[metaStart:metaStart+4], uint32(len(meta)))
	copy(data[metaStart+4:], meta)

	binary.BigEndian.PutUint32(data[payLoadStart:payLoadStart+4], uint32(len(payload)))
	copy(data[payLoadStart+4:], payload)

	return data
}

// WriteTo writes message to writers.
func (m Message) WriteTo(w io.Writer) error {
	_, err := w.Write(m.Header[:]) // 包头
	if err != nil {
		return err
	}

	meta := encodeMetadata(m.Metadata) // metadata

	spL := len(m.ServicePath)
	smL := len(m.ServiceMethod)

	payload := m.Payload
	if m.CompressType() != None { // 根据指定压缩包类型 来解压缩payload
		compressor := Compressors[m.CompressType()]
		if compressor == nil {
			return ErrUnsupportedCompressor
		}
		payload, err = compressor.Zip(m.Payload)
		if err != nil {
			return err
		}
	}

	// 使用大端序模式
	totalL := (4 + spL) + (4 + smL) + (4 + len(meta)) + (4 + len(payload))
	err = binary.Write(w, binary.BigEndian, uint32(totalL)) // 消息体大小
	if err != nil {
		return err
	}

	//write servicePath and serviceMethod
	err = binary.Write(w, binary.BigEndian, uint32(len(m.ServicePath))) // 服务名长度
	if err != nil {
		return err
	}
	_, err = w.Write(util.StringToSliceByte(m.ServicePath)) // 服务名
	if err != nil {
		return err
	}
	err = binary.Write(w, binary.BigEndian, uint32(len(m.ServiceMethod))) // 服务函数名
	if err != nil {
		return err
	}
	_, err = w.Write(util.StringToSliceByte(m.ServiceMethod)) // 服务函数名
	if err != nil {
		return err
	}

	// write meta
	err = binary.Write(w, binary.BigEndian, uint32(len(meta))) // meta大小
	if err != nil {
		return err
	}
	_, err = w.Write(meta) // meta内容
	if err != nil {
		return err
	}

	//write payload
	err = binary.Write(w, binary.BigEndian, uint32(len(payload))) // payload大小
	if err != nil {
		return err
	}

	_, err = w.Write(payload) // payload内容
	return err
}

// 编码metadata：<len,string>,<len,string>... 基本上len(key),key;len(value),value对出现
// len,string,len,string,......
func encodeMetadata(m map[string]string) []byte {
	if len(m) == 0 { // 未提供meta部分 用[]byte{}代替
		return []byte{}
	}
	var buf bytes.Buffer
	var d = make([]byte, 4)
	for k, v := range m { // 也是使用大端序模式
		binary.BigEndian.PutUint32(d, uint32(len(k))) // key
		buf.Write(d)
		buf.Write(util.StringToSliceByte(k))
		binary.BigEndian.PutUint32(d, uint32(len(v))) // value
		buf.Write(d)
		buf.Write(util.StringToSliceByte(v))
	}
	return buf.Bytes()
}

// 解码meta部分
func decodeMetadata(l uint32, data []byte) (map[string]string, error) {
	m := make(map[string]string, 10)
	n := uint32(0)
	for n < l {
		// parse one key and value
		// key
		sl := binary.BigEndian.Uint32(data[n : n+4])
		n = n + 4
		if n+sl > l-4 {
			return m, ErrMetaKVMissing
		}
		k := string(data[n : n+sl])
		n = n + sl

		// value
		sl = binary.BigEndian.Uint32(data[n : n+4])
		n = n + 4
		if n+sl > l {
			return m, ErrMetaKVMissing
		}
		v := string(data[n : n+sl])
		n = n + sl
		m[k] = v
	}

	return m, nil
}

// 读Message
// Read reads a message from r.
func Read(r io.Reader) (*Message, error) {
	msg := NewMessage()
	err := msg.Decode(r)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// 解码message
// Decode decodes a message from reader.
func (m *Message) Decode(r io.Reader) error {
	// validate rest length for each step?

	// 解析数据包头
	// parse header
	_, err := io.ReadFull(r, m.Header[:1])
	if err != nil {
		return err
	}
	if !m.Header.CheckMagicNumber() { // 验证数据包头中的魔数
		return fmt.Errorf("wrong magic number: %v", m.Header[0])
	}

	_, err = io.ReadFull(r, m.Header[1:]) //
	if err != nil {
		return err
	}

	//total
	lenData := poolUint32Data.Get().(*[]byte) // 数据包体 内容长度
	_, err = io.ReadFull(r, *lenData)
	if err != nil {
		poolUint32Data.Put(lenData)
		return err
	}
	l := binary.BigEndian.Uint32(*lenData)
	poolUint32Data.Put(lenData)

	if MaxMessageLength > 0 && int(l) > MaxMessageLength {
		return ErrMessageTooLong
	}

	totalL := int(l)
	if cap(m.data) >= totalL { //reuse data
		m.data = m.data[:totalL] // 提取消息中service、serviceMethod、metaData、payLoad
	} else {
		m.data = make([]byte, totalL)
	}
	data := m.data
	_, err = io.ReadFull(r, data)
	if err != nil {
		return err
	}

	// 解析servicePath部分：长度及内容
	n := 0
	// parse servicePath
	l = binary.BigEndian.Uint32(data[n:4])
	n = n + 4
	nEnd := n + int(l)
	m.ServicePath = util.SliceByteToString(data[n:nEnd])
	n = nEnd

	// 解析serviceP部分:长度及内容
	// parse serviceMethod
	l = binary.BigEndian.Uint32(data[n : n+4])
	n = n + 4
	nEnd = n + int(l)
	m.ServiceMethod = util.SliceByteToString(data[n:nEnd])
	n = nEnd

	// 解析metaData部分：长度及内容
	// parse meta
	l = binary.BigEndian.Uint32(data[n : n+4])
	n = n + 4
	nEnd = n + int(l)

	if l > 0 {
		m.Metadata, err = decodeMetadata(l, data[n:nEnd])
		if err != nil {
			return err
		}
	}
	n = nEnd

	// 解析payLoad部分：长度及内容；该部分解析注意压缩类型，需要根据对应的压缩类型进行解压缩
	// parse payload
	l = binary.BigEndian.Uint32(data[n : n+4])
	_ = l
	n = n + 4
	m.Payload = data[n:]

	if m.CompressType() != None {
		compressor := Compressors[m.CompressType()]
		if compressor == nil {
			return ErrUnsupportedCompressor
		}
		m.Payload, err = compressor.Unzip(m.Payload)
		if err != nil {
			return err
		}
	}

	return err
}

// 清空Message内容
// Reset clean data of this message but keep allocated data
func (m *Message) Reset() {
	resetHeader(m.Header)
	m.Metadata = nil
	m.Payload = []byte{}
	m.data = m.data[:0]
	m.ServicePath = ""
	m.ServiceMethod = ""
}

// 抛弃数据包头中的魔数部分的内容
var zeroHeaderArray Header
var zeroHeader = zeroHeaderArray[1:]

func resetHeader(h *Header) {
	copy(h[1:], zeroHeader)
}
