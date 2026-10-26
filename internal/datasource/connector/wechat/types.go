// Package wechat implements the WeChat Official Account (微信公众号) data source
// connector for WeKnora.
//
// WeChat does not offer a public API for reading arbitrary official accounts, so
// this connector is built on the Sogou WeChat search index (weixin.sogou.com),
// which is the only source an unattended server can read without credentials.
// Two kinds of subscription are supported:
//
//   - accounts:  one query per 公众号 name; results are kept only when the
//     article's account column matches the configured name.
//   - keywords:  one query per keyword; the newest Y in-window articles are kept.
//
// Both modes are bounded by a rolling window (the "last X months" setting) and
// an incremental cursor that records which articles have already been ingested,
// so a re-sync uploads only genuinely new articles and never duplicates one that
// surfaced under two different keywords.
//
// The upstream is rate limited and occasionally answers with an anti-spider
// interstitial instead of results. That case is surfaced as a dedicated error
// (see ErrBlocked) so the UI can tell the user to retry later or supply a
// browser Cookie, instead of reporting "0 new articles" as if the sync worked.
package wechat

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
)

const (
	// DefaultBaseURL is the Sogou WeChat search host. Only the host is
	// configurable: the request shape (/weixin?type=2…) is baked into the client.
	DefaultBaseURL = "https://weixin.sogou.com"

	// DefaultMonths is the look-back window used when the user did not set one.
	DefaultMonths = 3
	// MaxMonths caps the look-back window (three years); anything wider is not
	// meaningfully indexed by the upstream anyway.
	MaxMonths = 36

	// DefaultTopN is how many keyword results are synced per keyword.
	DefaultTopN = 50
	// MaxTopN caps keyword results per keyword.
	MaxTopN = 500

	// ResourceID prefixes used by ListResources. The saved selection is the
	// subset of subscriptions the user ticked in the resource picker.
	ResourcePrefixAccount = "account:"
	ResourcePrefixKeyword = "keyword:"
)

// Config holds WeChat-specific configuration.
//
// Accounts / Keywords / Months / TopN / BaseURL are non-secret and therefore live
// in DataSourceConfig.Settings. Cookie is a browser session cookie that may carry
// a login identity, so it belongs to Credentials and is encrypted at rest.
// Credentials may still carry the whole struct: the "test connection" endpoint
// (POST /datasource/validate-credentials) only ever receives a credentials map,
// so the UI passes everything there for a credential-less dry run.
type Config struct {
	// Accounts are 公众号 display names or IDs, one per subscription.
	Accounts stringList `json:"accounts,omitempty"`

	// Keywords are free-text queries; each one syncs its newest TopN articles.
	Keywords stringList `json:"keywords,omitempty"`

	// Months is the look-back window X in months (1..MaxMonths).
	Months int `json:"months,omitempty"`

	// TopN is the per-keyword cap Y (1..MaxTopN).
	TopN int `json:"top_n,omitempty"`

	// BaseURL overrides the upstream host (self-hosted proxy / mirror).
	BaseURL string `json:"base_url,omitempty"`

	// Cookie is an optional browser Cookie header value used to avoid the
	// upstream's anti-spider interstitial.
	Cookie string `json:"cookie,omitempty"`
}

// GetBaseURL returns the normalized base URL (empty → default, no trailing slash).
func (c *Config) GetBaseURL() string {
	url := strings.TrimSpace(c.BaseURL)
	if url == "" {
		return DefaultBaseURL
	}
	if !strings.Contains(url, "://") {
		url = "https://" + url
	}
	return strings.TrimRight(url, "/")
}

// Window returns the [since, until] pair described by Months.
func (c *Config) Window(now time.Time) (time.Time, time.Time) {
	months := c.Months
	if months <= 0 {
		months = DefaultMonths
	}
	return now.AddDate(0, -months, 0), now
}

// parseConfig extracts and validates WeChat configuration.
//
// Credentials are decoded first (that is all the validate-credentials endpoint
// gets), then overwritten by Settings, which is where the editor stores the
// non-secret values for a saved data source.
func parseConfig(config *types.DataSourceConfig) (*Config, error) {
	if config == nil {
		return nil, fmt.Errorf("%w: config is nil", datasource.ErrInvalidConfig)
	}

	credBytes, err := json.Marshal(config.Credentials)
	if err != nil {
		return nil, fmt.Errorf("marshal credentials: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(credBytes, &cfg); err != nil {
		return nil, fmt.Errorf("parse wechat credentials: %w", err)
	}

	applySettings(&cfg, config.Settings)

	cfg.Accounts = stringList(cleanList([]string(cfg.Accounts)))
	cfg.Keywords = stringList(cleanList([]string(cfg.Keywords)))
	cfg.Cookie = strings.TrimSpace(cfg.Cookie)

	if cfg.Months <= 0 {
		cfg.Months = DefaultMonths
	}
	if cfg.Months > MaxMonths {
		return nil, fmt.Errorf("%w: months must be between 1 and %d", datasource.ErrInvalidConfig, MaxMonths)
	}
	if cfg.TopN <= 0 {
		cfg.TopN = DefaultTopN
	}
	if cfg.TopN > MaxTopN {
		return nil, fmt.Errorf("%w: top_n must be between 1 and %d", datasource.ErrInvalidConfig, MaxTopN)
	}
	if len(cfg.Accounts) == 0 && len(cfg.Keywords) == 0 {
		return nil, fmt.Errorf(
			"%w:至少填写一个公众号或一个关键词",
			datasource.ErrInvalidConfig,
		)
	}
	if err := datasource.ValidateConnectorBaseURL(cfg.GetBaseURL()); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// applySettings overlays the non-secret values stored in DataSourceConfig.Settings.
// A setting that is absent or empty never clears a value that came from
// credentials, so the validate-credentials dry run and a saved row behave alike.
func applySettings(cfg *Config, settings map[string]interface{}) {
	if len(settings) == 0 {
		return
	}
	if v := settingsStringList(settings, "accounts"); len(v) > 0 {
		cfg.Accounts = stringList(v)
	}
	if v := settingsStringList(settings, "keywords"); len(v) > 0 {
		cfg.Keywords = stringList(v)
	}
	if v, ok := settingsInt(settings, "months"); ok {
		cfg.Months = v
	}
	if v, ok := settingsInt(settings, "top_n"); ok {
		cfg.TopN = v
	}
	if v, ok := settings["base_url"]; ok {
		if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
			cfg.BaseURL = s
		}
	}
}

// settingsStringList reads a list that the UI may send as a JSON array or as
// newline/comma separated text (older rows and the textarea form both exist).
func settingsStringList(settings map[string]interface{}, key string) []string {
	raw, ok := settings[key]
	if !ok {
		return nil
	}
	switch v := raw.(type) {
	case string:
		return splitList(v)
	case []string:
		return v
	case []interface{}:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

// settingsInt reads an integer setting that JSON delivered as a number.
func settingsInt(settings map[string]interface{}, key string) (int, bool) {
	raw, ok := settings[key]
	if !ok {
		return 0, false
	}
	switch v := raw.(type) {
	case float64:
		return int(v), true
	case int:
		return v, true
	case string:
		var n int
		if _, err := fmt.Sscanf(strings.TrimSpace(v), "%d", &n); err == nil {
			return n, true
		}
	}
	return 0, false
}

// splitList splits newline / comma / full-width comma separated text.
func splitList(raw string) []string {
	fields := strings.FieldsFunc(raw, func(r rune) bool {
		return r == '\n' || r == '\r' || r == ',' || r == '，' || r == ';' || r == '；'
	})
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		out = append(out, f)
	}
	return out
}

// stringList is a []string that also accepts a single newline / comma separated
// string. The editor sends arrays, but a hand-written API call (or an older row)
// may carry text, and a decode error there would take the whole sync down.
type stringList []string

// UnmarshalJSON accepts a JSON array or a separated string.
func (s *stringList) UnmarshalJSON(data []byte) error {
	var arr []string
	if err := json.Unmarshal(data, &arr); err == nil {
		*s = arr
		return nil
	}
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}
	*s = splitList(text)
	return nil
}

// MarshalJSON always emits an array so the stored cursor / config stays typed.
func (s stringList) MarshalJSON() ([]byte, error) {
	if s == nil {
		return []byte("null"), nil
	}
	return json.Marshal([]string(s))
}

// cleanList trims, drops blanks and dedupes (order preserved, case-insensitive).
func cleanList(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		item := strings.TrimSpace(raw)
		if item == "" {
			continue
		}
		key := strings.ToLower(item)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	return out
}

// target is one subscription to sync: either an account or a keyword.
type target struct {
	kind  string // "account" | "keyword"
	value string
	key   string // resource_id, e.g. "keyword:牛市"
}

func (t target) isAccount() bool { return t.kind == "account" }

// targets resolves which subscriptions to sync.
//
// ⚠️ 订阅范围**以配置为准**，`resourceIDs` 只用来判断"上次保存的选择是否还认得出来"。
//
// 曾经按 `resourceIDs` 收窄（只同步被勾中的那几项），结果是个真实的漏同步 bug：
// 用户在编辑数据源时改了公众号/关键词列表，WeKnora 里 `resource_ids` 仍是上一次
// 保存时的镜像——只要其中**还有一项**对得上，收窄就会成立，于是**新增的公众号和
// 关键词永远同步不到**（现象就是"配好了但一篇都没进来"，而且日志里还写着 success）。
//
// 这个连接器的资源本来就是配置的镜像（`ListResources` 不发请求、直接按配置生成），
// 需求也是"把这些公众号/关键词的全部新文章同步进来"，所以正确语义是：配置写了什么
// 就同步什么，勾选不作为收窄依据。
func (c *Config) targets(ctx context.Context, resourceIDs []string) []target {
	all := make([]target, 0, len(c.Accounts)+len(c.Keywords))
	for _, a := range c.Accounts {
		all = append(all, target{kind: "account", value: a, key: ResourcePrefixAccount + a})
	}
	for _, k := range c.Keywords {
		all = append(all, target{kind: "keyword", value: k, key: ResourcePrefixKeyword + k})
	}
	if len(resourceIDs) > 0 && len(resourceIDs) != len(all) {
		// 只记一条线索，不做收窄（原因见函数注释）；保留参数是为了将来若真要做
		// 显式收窄，能在这里一眼看出"选择与配置不一致"这个前提。
		logger.Infof(
			ctx, "[WeChat] 保存的资源选择（%d 项）与当前订阅配置（%d 项）不一致，按配置全量同步",
			len(resourceIDs), len(all),
		)
	}
	return all
}

// article is one search hit as parsed off the upstream results page.
type article struct {
	// DocID is the upstream's per-result id (the <li d="…"> attribute). It is
	// stable per indexed article and is used to dedupe within one page set even
	// when two results point at the same canonical URL.
	DocID string

	Title     string
	Summary   string
	Account   string
	Link      string // absolute upstream /link?url=… URL
	Published time.Time
}

// cursor stores incremental sync state: every article already ingested, keyed by
// external id, mapped to its publish time (used to trim the map).
type cursor struct {
	LastSyncTime time.Time        `json:"last_sync_time"`
	Seen         map[string]int64 `json:"seen,omitempty"`
}

// maxSeenEntries bounds the cursor. Articles older than the widest window can
// never be re-emitted, so the oldest entries are dropped first.
const maxSeenEntries = 20000

type seenEntry struct {
	id string
	ts int64
}

func (c *cursor) trim() {
	if c == nil || len(c.Seen) <= maxSeenEntries {
		return
	}
	entries := make([]seenEntry, 0, len(c.Seen))
	for id, ts := range c.Seen {
		entries = append(entries, seenEntry{id, ts})
	}
	// Keep the newest maxSeenEntries; older ones can never re-enter a window.
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ts != entries[j].ts {
			return entries[i].ts > entries[j].ts
		}
		return entries[i].id < entries[j].id
	})
	for _, e := range entries[maxSeenEntries:] {
		delete(c.Seen, e.id)
	}
}

// externalID derives a stable knowledge external_id for one hit.
//
// Identity must be computable BEFORE the article URL is resolved, because the
// sync skips already-ingested articles without paying the upstream hop that would
// reveal the canonical URL. The account + title + publish-second triple is stable
// across searches (the upstream's own per-result ids are not guaranteed to be).
// A hit without a usable publish time falls back to that upstream id, which is
// also stable, so repeated syncs still converge on one knowledge item.
func (a article) externalID() string {
	title := strings.TrimSpace(a.Title)
	account := strings.TrimSpace(a.Account)
	if !a.Published.IsZero() {
		sum := sha256.Sum256([]byte(account + "\x00" + title + "\x00" + strconv.FormatInt(a.Published.Unix(), 10)))
		return "wechat:" + hex.EncodeToString(sum[:])[:24]
	}
	if a.DocID != "" {
		return "wechat:doc:" + a.DocID
	}
	sum := sha256.Sum256([]byte(a.Link))
	return "wechat:" + hex.EncodeToString(sum[:])[:24]
}
