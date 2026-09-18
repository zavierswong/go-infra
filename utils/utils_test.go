package utils

import "testing"

func TestSanitizeDSN(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "amqp 带密码",
			in:   "amqp://guest:secret@127.0.0.1:5672/app",
			want: "amqp://guest:***@127.0.0.1:5672/app",
		},
		{
			name: "amqps 带密码",
			in:   "amqps://user:p%40ss@mq.example.com:5671/",
			want: "amqps://user:***@mq.example.com:5671/",
		},
		{
			name: "mysql 无 scheme",
			in:   "root:secret@tcp(127.0.0.1:3306)/app?charset=utf8mb4",
			want: "root:***@tcp(127.0.0.1:3306)/app?charset=utf8mb4",
		},
		{
			name: "无密码原样返回",
			in:   "amqp://guest@127.0.0.1:5672/app",
			want: "amqp://guest@127.0.0.1:5672/app",
		},
		{
			name: "无 @ 原样返回",
			in:   "127.0.0.1:6379",
			want: "127.0.0.1:6379",
		},
		{
			name: "空字符串",
			in:   "",
			want: "",
		},
		{
			name: "密码含未转义的 @（旧实现会漏出后半段）",
			in:   "amqp://guest:p@ss@127.0.0.1:5672/",
			want: "amqp://guest:***@127.0.0.1:5672/",
		},
		{
			name: "密码含多个 @",
			in:   "postgres://app:a@b@c@db.internal:5432/app",
			want: "postgres://app:***@db.internal:5432/app",
		},
		{
			name: "query 含 @ 且无 userinfo（不应改动）",
			in:   "redis://127.0.0.1:6379/0?opt=a@b",
			want: "redis://127.0.0.1:6379/0?opt=a@b",
		},
		{
			name: "IPv6 host 带密码",
			in:   "redis://app:secret@[::1]:6379/0",
			want: "redis://app:***@[::1]:6379/0",
		},
		{
			name: "userinfo 无密码（不应改动）",
			in:   "mysql://app@127.0.0.1:3306/db",
			want: "mysql://app@127.0.0.1:3306/db",
		},
		{
			name: "fragment 终止 authority",
			in:   "http://u:p@host:8080#frag@x",
			want: "http://u:***@host:8080#frag@x",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := SanitizeDSN(c.in); got != c.want {
				t.Errorf("SanitizeDSN(%q)\n 期望: %q\n 实际: %q", c.in, c.want, got)
			}
		})
	}
}
