package protocol

import (
	"github.com/smallnest/rpcx/util"
)

// 通用的压缩接口
// Compressor defines a common compression interface.
type Compressor interface {
	Zip([]byte) ([]byte, error)   // 压缩
	Unzip([]byte) ([]byte, error) // 解压
}

// GzipCompressor implements gzip compressor.
type GzipCompressor struct { // GZIP
}

func (c GzipCompressor) Zip(data []byte) ([]byte, error) {
	return util.Zip(data)
}

func (c GzipCompressor) Unzip(data []byte) ([]byte, error) {
	return util.Unzip(data)
}

type RawDataCompressor struct { // Raw压缩；对元数据内容不进行任何处理 直接返回即可
}

func (c RawDataCompressor) Zip(data []byte) ([]byte, error) {
	return data, nil
}

func (c RawDataCompressor) Unzip(data []byte) ([]byte, error) {
	return data, nil
}
