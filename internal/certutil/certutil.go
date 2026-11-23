package certutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// 确定性时间锚点：所有自签证书使用固定 NotBefore，避免墙上时间影响输出。
var epoch = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// CA 持有一个本地生成的证书颁发实体（证书 + 私钥）。
type CA struct {
	Cert *x509.Certificate
	Key  *ecdsa.PrivateKey
}

// ParseChain 解析 PEM 串中的完整证书链，返回顺序保持的证书切片。
// 结构不完好（解码失败、非证书、空链）时返回错误。
func ParseChain(pemStr string) ([]*x509.Certificate, error) {
	rest := []byte(pemStr)
	var chain []*x509.Certificate
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			if len(strings.TrimSpace(string(rest))) != 0 {
				return nil, errors.New("PEM 解码后仍存在无法识别的数据")
			}
			break
		}
		if block.Type != "CERTIFICATE" {
			return nil, fmt.Errorf("PEM 块类型异常: %s", block.Type)
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("证书解析失败: %w", err)
		}
		chain = append(chain, cert)
	}
	if len(chain) == 0 {
		return nil, errors.New("证书链为空或 PEM 结构损坏")
	}
	return chain, nil
}

// EncodeChain 将证书切片编码为 PEM 串。
func EncodeChain(chain []*x509.Certificate) string {
	out := ""
	for _, cert := range chain {
		out += string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}))
	}
	return out
}

// ParseCert 解析单张 PEM 证书。
func ParseCert(pemStr string) (*x509.Certificate, error) {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, errors.New("PEM 证书解码失败")
	}
	return x509.ParseCertificate(block.Bytes)
}

// EncodeCert 编码单张证书为 PEM。
func EncodeCert(cert *x509.Certificate) string {
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}))
}

// newKey 生成 ECDSA P-256 密钥。密钥生成使用加密随机源，
// 但所有校验只依赖证书内容，随机源不影响报告确定性。
func newKey() (*ecdsa.PrivateKey, error) {
	return ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
}

// NewRoot 生成自签根 CA，serial 决定序列号，保证可重复构造。
func NewRoot(serial int64, commonName string) (*CA, error) {
	key, err := newKey()
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               subject(commonName),
		NotBefore:             epoch,
		NotAfter:              epoch.AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &CA{Cert: cert, Key: key}, nil
}

// NewIntermediate 由父 CA 签发中间 CA。
func NewIntermediate(serial int64, commonName string, parent *CA) (*CA, error) {
	key, err := newKey()
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               subject(commonName),
		NotBefore:             epoch,
		NotAfter:              epoch.AddDate(5, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent.Cert, &key.PublicKey, parent.Key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &CA{Cert: cert, Key: key}, nil
}

// NewLeaf 由签发者（中间或根）签发叶证书，SAN 决定 DNS/IP 名称集合。
func NewLeaf(serial int64, commonName string, sans []string, issuer *CA) (*x509.Certificate, error) {
	key, err := newKey()
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      subject(commonName),
		NotBefore:    epoch,
		NotAfter:     epoch.AddDate(100, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     sans,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, issuer.Cert, &key.PublicKey, issuer.Key)
	if err != nil {
		return nil, err
	}
	return x509.ParseCertificate(der)
}

// SPKIDigest 计算证书 SubjectPublicKeyInfo 的摘要并返回 base64 标准编码。
func SPKIDigest(cert *x509.Certificate, algo string) (string, error) {
	var sum []byte
	switch algo {
	case "sha256":
		d := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
		sum = d[:]
	case "sha384":
		d := sha512.Sum384(cert.RawSubjectPublicKeyInfo)
		sum = d[:]
	default:
		return "", fmt.Errorf("unsupported hash algorithm %q", algo)
	}
	return base64.StdEncoding.EncodeToString(sum), nil
}

// SupportedAlgo 报告哈希算法是否受支持。
func SupportedAlgo(algo string) bool {
	return algo == "sha256" || algo == "sha384"
}

// LeafFingerprint 返回叶证书整体 DER 的 sha256 指纹（hex），用于 MITM 取证。
func LeafFingerprint(cert *x509.Certificate) string {
	d := sha256.Sum256(cert.Raw)
	return fmt.Sprintf("%x", d[:])
}

// VerifyChain 以给定根集合构建并验证证书路径，返回验证到的链数量。
// roots 为受信根证书切片；intermediates 取自观测链中除叶与根之外的证书。
func VerifyChain(leaf *x509.Certificate, intermediates []*x509.Certificate, roots []*x509.Certificate, serverName string) (int, error) {
	pool := x509.NewCertPool()
	for _, root := range roots {
		pool.AddCert(root)
	}
	interPool := x509.NewCertPool()
	for _, inter := range intermediates {
		interPool.AddCert(inter)
	}
	opts := x509.VerifyOptions{
		Roots:         pool,
		Intermediates: interPool,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if serverName != "" {
		opts.DNSName = serverName
	}
	chains, err := leaf.Verify(opts)
	if err != nil {
		return 0, err
	}
	return len(chains), nil
}

// SANMatches 判断 serverName 是否出现在证书 SAN（DNS 名）中，兼容 CN。
func SANMatches(cert *x509.Certificate, serverName string) bool {
	if serverName == "" {
		return true
	}
	for _, name := range cert.DNSNames {
		if name == serverName {
			return true
		}
	}
	if len(cert.DNSNames) == 0 && cert.Subject.CommonName == serverName {
		return true
	}
	return false
}

// FingerprintCert 对任意证书返回 sha256 hex 指纹。
func FingerprintCert(cert *x509.Certificate) string {
	return LeafFingerprint(cert)
}

// PublicKeyAlgoName 返回公钥算法的稳定名称。
func PublicKeyAlgoName(cert *x509.Certificate) string {
	switch cert.PublicKeyAlgorithm {
	case x509.ECDSA:
		return "ECDSA"
	case x509.RSA:
		return "RSA"
	case x509.Ed25519:
		return "Ed25519"
	default:
		return "UNKNOWN"
	}
}

// Certificate 是标准库 x509 证书的别名，便于外部包统一引用。
type Certificate = x509.Certificate

// CheckChainStructure 校验观测链的结构完整性：除根外每张证书必须由链中下一张
// 证书直接签发（签名关系连续），且末张必须是自签根。中间证书缺失即判定链断裂。
func CheckChainStructure(chain []*x509.Certificate) error {
	if len(chain) == 0 {
		return errors.New("证书链为空")
	}
	for i := 0; i < len(chain)-1; i++ {
		child, parent := chain[i], chain[i+1]
		if child.CheckSignatureFrom(parent) != nil {
			return fmt.Errorf("证书 %s 并非由链中下一张证书 %s 直接签发，链结构不连续",
				child.Subject.CommonName, parent.Subject.CommonName)
		}
	}
	root := chain[len(chain)-1]
	if root.CheckSignatureFrom(root) != nil {
		return fmt.Errorf("链末张证书 %s 不是自签根", root.Subject.CommonName)
	}
	return nil
}
