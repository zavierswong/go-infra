package mongo

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// 本文件负责解析与校验 MongoDB 连接串（URI），并把它**脱敏**后用于日志。
//
// # 为什么自己解析一遍
//
// 驱动（connstring.ParseAndValidate）当然也会解析，而且它才是最终权威 ——
// 本包在 Config 校验与建连时都会走驱动的校验。自己再解析一遍只为了三件驱动不做的事：
//
//  1. **启动期给出可读的错误**。驱动的报错是英文且偏底层
//     （例如 `error parsing uri: scheme must be "mongo" or "mongo+srv"`），
//     本包补上"哪里写错了、该怎么写"的上下文。
//  2. **脱敏**。URI 里的密码不止 userinfo 一处，
//     `tlsCertificateKeyFilePassword` 也是口令，只抹 userinfo 是不够的。
//  3. **取出默认库名**，供 `MongoDB.DefaultDatabase()` 使用。
//
// # 两种 scheme
//
//	mongo://host1:27017,host2:27017/db?replicaSet=rs0   # 直连种子列表
//	mongo+srv://cluster0.example.com/db?retryWrites=true  # 走 DNS SRV 发现节点
//
// 关键差别：SRV 形式**不能写端口**（端口只能由 DNS 记录给出），
// 写了驱动会直接报错，本包在解析阶段就拦下并说明原因。

const (
	// schemeMongoDB 是标准连接串（本包对外的写法）。
	schemeMongoDB = "mongo"
	// schemeMongoDBSRV 是走 DNS SRV 记录的连接串。
	schemeMongoDBSRV = "mongo+srv"

	// 驱动的 connstring 解析只认这两种 scheme —— 它不接受 mongo://。
	// 本包对外统一用更短的 mongo://，交给驱动前必须归一化成下面这两种。
	schemeDriverMongoDB    = "mongodb"
	schemeDriverMongoDBSRV = "mongodb+srv"

	// redacted 是写入日志时替换口令的占位符。
	redacted = "***"
)

// sensitiveQueryKeys 是"取值是口令"的查询参数，脱敏时必须一并抹掉。
//
// 起因很具体：URI 的 userinfo 只是口令的一个入口，
// `?tlsCertificateKeyFilePassword=...` 同样带口令（用于打开客户端证书私钥）。
// 只处理 userinfo 的脱敏函数会把私钥口令原样写进日志。
//
// 键必须小写 —— MongoDB 的 URI 参数名是**大小写不敏感**的。
var sensitiveQueryKeys = map[string]struct{}{
	"tlscertificatekeyfilepassword":   {},
	"sslclientcertificatekeypassword": {},
}

// uriInfo 是解析后的连接串。
type uriInfo struct {
	// raw 是原样输入的连接串。**含密码**，不要直接记录到日志。
	raw string
	// url 是标准库的解析结果，保留原始大小写与转义形式。
	url *url.URL
	// scheme 是已小写化的 scheme。
	scheme string
	// hosts 是展开后的主机列表（已去掉空白）。
	hosts []string
	// dbName 是路径里的默认库名，可能为空。
	dbName string
	// params 是查询参数，键已小写（MongoDB 的参数名大小写不敏感）。
	params map[string]string
}

// parseURI 解析并做**结构性**校验。
//
// 这里刻意不重复驱动已经做得很好的校验（minPoolSize ≤ maxPoolSize、
// directConnection 与多主机/SRV 互斥、heartbeat 下限、loadBalanced 组合约束等）——
// 那些交给 options.ClientOptions.Validate()，本包只在它之前补结构性问题。
func parseURI(raw string) (*uriInfo, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, fmt.Errorf("%w: URI 不能为空", ErrInvalidConfig)
	}

	u, err := url.Parse(trimmed)
	if err != nil {
		// 最常见的具体原因是密码里有未转义的特殊字符（@ : / ? # %）。
		// MongoDB 要求这些字符在 userinfo 里做百分号编码。
		return nil, fmt.Errorf(
			"%w: URI 解析失败: %v（若密码含 @ : / ? # %% 等字符，需做百分号编码）",
			ErrInvalidConfig, err)
	}

	scheme := strings.ToLower(u.Scheme)
	if scheme != schemeMongoDB && scheme != schemeMongoDBSRV {
		return nil, fmt.Errorf(
			"%w: 不支持的 scheme %q，只能是 %q 或 %q",
			ErrInvalidConfig, u.Scheme, schemeMongoDB, schemeMongoDBSRV)
	}

	hosts, err := parseHosts(u, scheme)
	if err != nil {
		return nil, err
	}

	dbName, err := parseDBName(u)
	if err != nil {
		return nil, err
	}

	params, err := parseParams(u)
	if err != nil {
		return nil, err
	}

	return &uriInfo{
		raw:    trimmed,
		url:    u,
		scheme: scheme,
		hosts:  hosts,
		dbName: dbName,
		params: params,
	}, nil
}

// driverURI 返回交给驱动的连接串：把本包的 mongo:// 归一化成驱动唯一
// 接受的 mongodb://，其余部分（userinfo、主机、路径、参数）原样保留。
//
// 少了这一层，ApplyURI 会以 "scheme must be mongodb" 失败，
// 任何按本包文档书写的 URI 都建不了连。
func (u *uriInfo) driverURI() string {
	sep := strings.Index(u.raw, "://")
	if sep < 0 {
		return u.raw
	}
	rest := u.raw[sep+len("://"):]
	switch u.scheme {
	case schemeMongoDB:
		return schemeDriverMongoDB + "://" + rest
	case schemeMongoDBSRV:
		return schemeDriverMongoDBSRV + "://" + rest
	default:
		return u.raw
	}
}

// parseHosts 拆出主机列表并逐个校验。
func parseHosts(u *url.URL, scheme string) ([]string, error) {
	if u.Host == "" {
		return nil, fmt.Errorf("%w: URI 未指定主机（形如 %s://host:27017）",
			ErrInvalidConfig, scheme)
	}

	// 标准库把 "h1:27017,h2:27017" 整体当成 Host，所以这里自己按逗号拆。
	parts := strings.Split(u.Host, ",")
	hosts := make([]string, 0, len(parts))
	for _, p := range parts {
		h := strings.TrimSpace(p)
		if h == "" {
			return nil, fmt.Errorf("%w: URI 的主机列表里有空项（多余的逗号？）: %q",
				ErrInvalidConfig, u.Host)
		}

		host, port, hasPort := splitHostPort(h)
		if host == "" {
			return nil, fmt.Errorf("%w: URI 里的主机名为空: %q", ErrInvalidConfig, h)
		}
		if !hasPort {
			hosts = append(hosts, host)
			continue
		}

		// SRV 形式的端口只能由 DNS 记录给出，写死端口是明确的错误用法。
		if scheme == schemeMongoDBSRV {
			return nil, fmt.Errorf(
				"%w: %s:// 不能指定端口（%q）—— 端口由 DNS SRV 记录给出；"+
					"要固定端口请改用 %s://",
				ErrInvalidConfig, schemeMongoDBSRV, h, schemeMongoDB)
		}

		// 端口必须是数字，且非数字端口在驱动里会被当成"主机名的一部分"而报错得很难懂。
		n, err := strconv.Atoi(port)
		if err != nil {
			return nil, fmt.Errorf("%w: 主机 %q 的端口 %q 不是数字", ErrInvalidConfig, host, port)
		}
		if n < 1 || n > 65535 {
			return nil, fmt.Errorf("%w: 主机 %q 的端口 %d 超出范围（1-65535）",
				ErrInvalidConfig, host, n)
		}
		hosts = append(hosts, h)
	}
	return hosts, nil
}

// splitHostPort 拆主机与端口。
//
// 不用 net.SplitHostPort：它要求端口必须存在，且对 IPv6 的裸地址
// （`::1` 没有方括号）会返回错误，而那种写法在 MongoDB URI 里是合法的
// （MongoDB 对 IPv6 用 `[::1]:27017`）。
// 这里按**最后一个冒号**拆，与驱动的处理一致；方括号形式直接整体当主机名。
func splitHostPort(h string) (host, port string, hasPort bool) {
	if i := strings.LastIndexByte(h, ':'); i >= 0 {
		// "[::1]:27017" 里最后一个冒号确实是端口分隔符。
		if strings.HasPrefix(h, "[") && strings.HasSuffix(h, "]") {
			return h, "", false
		}
		return h[:i], h[i+1:], true
	}
	return h, "", false
}

// parseDBName 取出路径里的默认库名并校验。
func parseDBName(u *url.URL) (string, error) {
	name := strings.TrimPrefix(u.Path, "/")
	if name == "" {
		return "", nil
	}

	// 路径分隔符出现两次说明写成了 `.../a/b`，那不是库名，是路径。
	if strings.Contains(name, "/") {
		return "", fmt.Errorf(
			"%w: 路径 %q 非法 —— 连接串的路径只能是**一个**默认库名，"+
				"不能是路径（形如 /dbname）", ErrInvalidConfig, u.Path)
	}

	// 数据库名里不能出现的字符。写成集合名或带空格的路径时最容易踩。
	if strings.ContainsAny(name, " \\\"$*<>:|?") {
		return "", fmt.Errorf(
			"%w: 默认库名 %q 含非法字符（不能有空格与 \\ \" $ * < > : | ?）",
			ErrInvalidConfig, name)
	}
	return name, nil
}

// parseParams 解析查询参数。
//
// 两个必须处理的细节：
//
//   - **参数名大小写不敏感**。`maxPoolSize` / `maxpoolsize` / `MAXPOOLSIZE`
//     是同一个参数，所以统一转小写后再建表。
//   - **大小写不同的重复参数要报错**。`?maxPoolSize=50&MAXPOOLSIZE=10`
//     在驱动里是"后者覆盖前者"（因为是按小写查表的），
//     这类配置几乎一定是复制粘贴事故，静默取其中一个会让人查很久。
func parseParams(u *url.URL) (map[string]string, error) {
	if u.RawQuery == "" {
		return nil, nil
	}

	// 不用 u.Query()：它对非法转义是**静默跳过**的，
	// 于是 `?x=%zz` 会悄悄消失，而不是报错。这里要拿到那个错误。
	values, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("%w: URI 查询参数转义非法: %v", ErrInvalidConfig, err)
	}

	params := make(map[string]string, len(values))
	for key, vals := range values {
		lower := strings.ToLower(key)
		if len(vals) > 1 {
			return nil, fmt.Errorf("%w: 查询参数 %q 重复出现", ErrInvalidConfig, key)
		}
		if _, dup := params[lower]; dup {
			return nil, fmt.Errorf(
				"%w: 查询参数 %q 与另一个参数大小写冲突（MongoDB 的参数名大小写不敏感，"+
					"只会取其中一个）", ErrInvalidConfig, key)
		}
		params[lower] = vals[0]
	}
	return params, nil
}

// has 判断连接串里是否显式指定了某个参数（键大小写不敏感）。
func (u *uriInfo) has(key string) bool {
	_, ok := u.params[strings.ToLower(key)]
	return ok
}

// get 取出参数值（键大小写不敏感）。
func (u *uriInfo) get(key string) (string, bool) {
	v, ok := u.params[strings.ToLower(key)]
	return v, ok
}

// sanitized 返回**抹掉全部口令**的连接串，可安全写入日志。
//
// 处理两处口令，缺一不可：
//   - userinfo 里的密码：`mongo://user:pass@host` → `mongo://user:***@host`
//   - 查询参数里的私钥口令：`?tlsCertificateKeyFilePassword=pass` → `...=***`
//
// 没有命中任何口令时**原样返回**（逐字节保留原始写法），
// 这样日志里看到的就是配置里写的那一行，方便直接 diff；
// 命中了才重新拼装查询串，因此那时的参数顺序会变成字典序 ——
// 这是刻意的取舍：宁可展示串与配置串顺序不同，也不能让口令泄漏。
//
// 注意它是**展示用**字符串，不保证可以拿去重新连接。
func (u *uriInfo) sanitized() string {
	if !u.hasSensitive() {
		return u.raw
	}

	var b strings.Builder
	b.WriteString(u.scheme)
	b.WriteString("://")
	if u.url.User != nil {
		// url.User(...).String() 会按 userinfo 规则做转义，
		// 于是用户名里的特殊字符不会被拼出非法 URI。
		b.WriteString(url.User(u.url.User.Username()).String())
		if _, hasPassword := u.url.User.Password(); hasPassword {
			b.WriteString(":" + redacted)
		}
		b.WriteByte('@')
	}
	b.WriteString(u.url.Host)
	b.WriteString(u.url.EscapedPath())

	if u.url.RawQuery != "" {
		// url.Values.Encode() 会对取值重新转义并按字典序输出。
		query := u.url.Query()
		for key := range query {
			if _, sensitive := sensitiveQueryKeys[strings.ToLower(key)]; sensitive {
				query.Set(key, redacted)
			}
		}
		b.WriteByte('?')
		b.WriteString(query.Encode())
	}
	return b.String()
}

// hasSensitive 判断连接串里是否真的带了口令。
func (u *uriInfo) hasSensitive() bool {
	if u.url.User != nil {
		if _, hasPassword := u.url.User.Password(); hasPassword {
			return true
		}
	}
	for key := range u.params {
		if _, sensitive := sensitiveQueryKeys[key]; sensitive {
			return true
		}
	}
	return false
}

// hostsString 返回逗号分隔的主机列表，用于日志与状态快照。
func (u *uriInfo) hostsString() string { return strings.Join(u.hosts, ",") }
