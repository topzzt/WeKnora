package wechat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
)

// Compile-time proof that *Connector satisfies the datasource.Connector interface.
var _ datasource.Connector = (*Connector)(nil)

// Connector implements datasource.Connector for WeChat Official Account
// subscriptions backed by the Sogou WeChat search index.
type Connector struct{}

// NewConnector creates a new WeChat connector.
func NewConnector() *Connector { return &Connector{} }

// Type returns the connector type identifier.
func (c *Connector) Type() string { return types.ConnectorTypeWeChat }

// Validate tests the upstream end to end with the configured subscriptions: it
// runs one real search (the first account, else the first keyword). A reachable
// index with zero hits is still a healthy connection — an account simply may not
// have published inside the window.
func (c *Connector) Validate(ctx context.Context, config *types.DataSourceConfig) error {
	cfg, err := parseConfig(config)
	if err != nil {
		return err
	}
	targets := cfg.targets(ctx, nil)
	if len(targets) == 0 {
		return fmt.Errorf("%w:至少填写一个公众号或一个关键词", datasource.ErrInvalidConfig)
	}

	cli := newClient(cfg)
	since, until := cfg.Window(time.Now())
	if _, err := cli.search(ctx, targets[0].value, 1, since, until); err != nil {
		if errors.Is(err, ErrBlocked) {
			return fmt.Errorf("%w；请在「Cookie（可选）」填入浏览器 Cookie，或稍后重试", err)
		}
		return err
	}
	return nil
}

// ResolveResourceAncestors has nothing to do: subscriptions are a flat list.
func (c *Connector) ResolveResourceAncestors(
	ctx context.Context, config *types.DataSourceConfig, resourceIDs []string,
) ([]string, error) {
	return []string{}, nil
}

// ListResources returns one resource per configured subscription so the picker
// can scope a sync to a subset. No network call is made: the subscriptions are
// already fully described by the configuration, and every extra request risks
// the upstream's rate limiter.
func (c *Connector) ListResources(
	ctx context.Context, config *types.DataSourceConfig, parentID string,
) ([]types.Resource, error) {
	if parentID != "" {
		return []types.Resource{}, nil
	}
	cfg, err := parseConfig(config)
	if err != nil {
		return nil, err
	}
	since, until := cfg.Window(time.Now())
	out := make([]types.Resource, 0, len(cfg.Accounts)+len(cfg.Keywords))
	for _, a := range cfg.Accounts {
		out = append(out, types.Resource{
			ExternalID:  ResourcePrefixAccount + a,
			Type:        "account",
			Name:        a,
			Description: "公众号 · 最近 " + strconv.Itoa(cfg.Months) + " 个月",
			Metadata:    map[string]interface{}{"kind": "account", "since": since.Format(time.DateOnly), "until": until.Format(time.DateOnly)},
		})
	}
	for _, k := range cfg.Keywords {
		out = append(out, types.Resource{
			ExternalID:  ResourcePrefixKeyword + k,
			Type:        "keyword",
			Name:        k,
			Description: "关键词 · 最新 " + strconv.Itoa(cfg.TopN) + " 篇",
			Metadata:    map[string]interface{}{"kind": "keyword", "top_n": cfg.TopN},
		})
	}
	return out, nil
}

// FetchAll re-reads every in-window article for the selected subscriptions. The
// knowledge base is upserted by external id, so a full sync refreshes bodies
// without creating duplicates.
func (c *Connector) FetchAll(
	ctx context.Context, config *types.DataSourceConfig, resourceIDs []string,
) ([]types.FetchedItem, error) {
	items, _, err := c.walk(ctx, config, resourceIDs, nil)
	return items, err
}

// FetchIncremental returns only articles that are not already recorded in the
// cursor. Deletions are intentionally not emitted: an article leaving the search
// index says nothing about it being withdrawn.
func (c *Connector) FetchIncremental(
	ctx context.Context, config *types.DataSourceConfig, cursor *types.SyncCursor,
) ([]types.FetchedItem, *types.SyncCursor, error) {
	prev := decodeCursor(ctx, cursor)
	items, next, err := c.walk(ctx, config, resourceIDsOf(config), prev)
	if next == nil {
		return items, nil, err
	}
	return items, next.toSyncCursor(), err
}

// resourceIDsOf returns the saved resource selection, tolerating a nil config.
func resourceIDsOf(config *types.DataSourceConfig) []string {
	if config == nil {
		return nil
	}
	return config.ResourceIDs
}

// walk is the shared implementation for FetchAll / FetchIncremental.
//
// prev carries the ids of articles already ingested. Emitting an item requires
// resolving its real article URL (one upstream hop), so the seen-check runs
// before the hop: a steady-state sync costs one search page per subscription and
// no hops at all.
func (c *Connector) walk(
	ctx context.Context,
	config *types.DataSourceConfig,
	resourceIDs []string,
	prev *cursor,
) ([]types.FetchedItem, *cursor, error) {
	cfg, err := parseConfig(config)
	if err != nil {
		return nil, nil, err
	}
	targets := cfg.targets(ctx, resourceIDs)
	if len(targets) == 0 {
		return nil, nil, fmt.Errorf("%w:没有可同步的公众号或关键词", datasource.ErrInvalidConfig)
	}

	cli := newClient(cfg)
	now := time.Now()
	since, until := cfg.Window(now)
	months := cfg.Months
	if months <= 0 {
		months = DefaultMonths
	}

	next := &cursor{LastSyncTime: now.UTC(), Seen: map[string]int64{}}
	if prev != nil {
		for id, ts := range prev.Seen {
			next.Seen[id] = ts
		}
	}

	var items []types.FetchedItem
	var failures []string
	skippedResolve := 0

	for _, t := range targets {
		// 关键词：先按候选取「前 Y 篇」，seen 留给 walk 过滤（否则 Y 会随增量滑动）。
		// 公众号：没有数量上限，直接在 collect 里跳过已入库条目，稳态一轮只有检索请求。
		var seenForCollect map[string]int64
		if t.isAccount() {
			seenForCollect = next.Seen
		}
		candidates, eligible, warnings, err := c.collect(ctx, cli, cfg, t, since, until, seenForCollect)
		// A page boundary failure still leaves earlier pages usable: the warning
		// is reported, the candidates keep flowing.
		failures = append(failures, warnings...)
		if err != nil {
			logger.Warnf(ctx, "[WeChat] %s: %v", t.key, err)
			failures = append(failures, fmt.Sprintf("%s: %v", t.key, err))
			continue
		}

		kept := 0
		for _, a := range candidates {
			id := a.externalID()
			if _, done := next.Seen[id]; done {
				continue
			}
			articleURL, err := cli.resolveLink(ctx, a.Link)
			if err != nil {
				if errors.Is(err, ErrBlocked) {
					failures = append(failures, fmt.Sprintf("%s: %v", t.key, err))
					break
				}
				// Not recorded as seen: the next sync retries this article
				// instead of silently losing it.
				skippedResolve++
				logger.Warnf(ctx, "[WeChat] %s: resolve %q failed: %v", t.key, a.Title, err)
				continue
			}
			next.Seen[id] = publishedUnix(a)
			items = append(items, types.FetchedItem{
				ExternalID:       id,
				Title:            a.Title,
				URL:              articleURL,
				ContentType:      "text/html",
				UpdatedAt:        a.Published,
				CreatedAt:        a.Published,
				SourceResourceID: t.key,
				Metadata: map[string]string{
					"channel":   "wechat",
					"account":   a.Account,
					"query":     t.value,
					"feed":      t.kind,
					"published": a.Published.Format(time.RFC3339),
				},
			})
			kept++
			if t.kind == "keyword" && kept >= cfg.TopN {
				break
			}
		}
		logger.Infof(ctx, "[WeChat] %s %q: candidates=%d new=%d", t.kind, t.value, len(candidates), kept)

		// 公众号订阅「时间窗内一条都没有」必须当失败报出来，不能算成功。
		//
		// 搜狗的「公众号搜索」(type=1) 已经彻底失效——2026-10-27 实测，央视新闻、
		// 券商中国、中国基金报全部返回「暂无相关的官方认证订阅号」，所以拿不到
		// 「该号文章列表」这个入口；而「文章搜索」(type=2) 是**相关度**排序，
		// query 填公众号名拿回来的是 2017–2024 年别人提到它的文章，本号近期文章
		// 根本不在结果里（tsn 时间过滤参数无论取 1..5 一律 302 反爬）。
		// 两边一夹，账号订阅很容易一条都拿不到。
		//
		// 判据是 eligible（窗口内的条数，不扣已见）而不是 len(candidates)：
		// eligible>0 且候选 0 只说明「全都已入库」，是稳态，必须继续报成功，
		// 否则每轮同步都会变成红。
		if t.isAccount() && eligible == 0 {
			failures = append(failures, fmt.Sprintf(
				"公众号「%s」：搜狗索引里取不到近 %d 个月的文章"+
					"（搜狗已无法按公众号取文章列表，只能按相关度返回旧文；"+
					"建议改用「关键词」订阅，或换用 RSS 数据源）",
				t.value, months))
		}
	}

	next.trim()

	if skippedResolve > 0 {
		note := fmt.Sprintf("%d 篇文章的原文地址解析失败，将在下次同步重试", skippedResolve)
		if len(items) == 0 {
			// 一篇都没拿到就只写 partial，界面会显示“同步成功、0 篇”，
			// 用户根本看不出是上游跳转页解析失败（本地/镜像上游最常见的形态）。
			// 这种情况必须当失败抛出来。
			return nil, next, fmt.Errorf("未能同步任何文章：%s", note)
		}
		failures = append(failures, note)
	}
	if len(failures) > 0 {
		if len(items) == 0 && len(failures) == len(targets) {
			return nil, next, fmt.Errorf("所有订阅均失败：%s", strings.Join(failures, "; "))
		}
		return items, next, &datasource.PartialFetchError{Details: failures}
	}
	return items, next, nil
}

// collect paginates one subscription and returns candidates that are inside the
// window. Pagination stops once a page contributes nothing new twice in a row
// (the index ranks by relevance, so an empty page is not a reliable end-of-results
// signal any more than a full one is a signal that older hits are exhausted).
//
// “seen“ may be nil: 传 nil 表示"先别做增量过滤"。**关键词订阅必须传 nil**——
// 见下方注释（TopN 截断点）。
//
// eligible 是本轮「符合订阅条件且在时间窗内」的条数（**不扣已见**）。调用方用它
// 区分两种「candidates=0」：eligible=0 说明上游根本没有该号在窗口内的文章（要报错）；
// eligible>0 只是全都已入库（稳态，属正常），两者绝不能混为一谈。
func (c *Connector) collect(
	ctx context.Context,
	cli *client,
	cfg *Config,
	t target,
	since, until time.Time,
	seen map[string]int64,
) (out []article, eligible int, warnings []string, err error) {
	out = make([]article, 0, 10)
	barren := 0
	dupInRun := make(map[string]struct{})

	for page := 1; page <= maxPagesPerTarget; page++ {
		articles, err := cli.search(ctx, t.value, page, since, until)
		if err != nil {
			if page == 1 {
				return nil, 0, nil, err
			}
			// Keep what earlier pages already produced; report the break so the
			// sync log shows a partial result instead of a silent truncation.
			return out, eligible, []string{fmt.Sprintf("%s:第 %d 页失败：%v", t.key, page, err)}, nil
		}
		if len(articles) == 0 {
			return out, eligible, nil, nil
		}

		added := 0
		for _, a := range articles {
			if t.isAccount() && !matchesAccount(a.Account, t.value) {
				continue
			}
			if !inWindow(a.Published, since, until) {
				continue
			}
			eligible++
			id := a.externalID()
			if seen != nil {
				if _, done := seen[id]; done {
					continue
				}
			}
			if _, dup := dupInRun[id]; dup {
				continue
			}
			dupInRun[id] = struct{}{}
			out = append(out, a)
			added++
			// ⚠️ 关键词的「前 Y 篇」截断必须落在**候选**上，不能落在"新增"上。
			// 曾经把已入库的条目在 collect 里滤掉再数 Y：首轮取前 5 篇，次轮前 5 篇
			// 都"已见"，于是从第 6 篇继续取 5 篇……每同步一次就往后挪一格，等于把
			// 整个结果集慢慢全部吞进来，永远收敛不到 0（用户看到"每次同步又新进几篇"）。
			// 所以关键词订阅在 collect 阶段不过滤 seen（调用方传 nil），
			// 见过的条目到 walk 里再跳过。
			if t.kind == "keyword" && len(out) >= cfg.TopN {
				return out, eligible, nil, nil
			}
		}
		if added == 0 {
			barren++
			if barren >= 2 {
				return out, eligible, nil, nil
			}
		} else {
			barren = 0
		}
	}
	return out, eligible, nil, nil
}

// inWindow reports whether an article publish time falls inside [since, until].
// An unparsable time (zero) is treated as in-window: the upstream occasionally
// renders a relative timestamp, and dropping the article outright would silently
// lose the freshest post — the very thing being synced. Duplicates are still
// prevented because such items fall back to their upstream document id.
func inWindow(published, since, until time.Time) bool {
	if published.IsZero() {
		return true
	}
	// The upstream is on Asia/Shanghai; treat the boundary generously by a day
	// so an article published just before midnight is not dropped.
	if published.Before(since.Add(-24 * time.Hour)) {
		return false
	}
	return !published.After(until.Add(24 * time.Hour))
}

// matchesAccount reports whether a hit belongs to the subscribed account. The
// upstream matches on the account field but returns other accounts' articles too,
// so this filter is what makes an account subscription mean what it says.
func matchesAccount(articleAccount, wanted string) bool {
	a := strings.ToLower(strings.TrimSpace(articleAccount))
	w := strings.ToLower(strings.TrimSpace(wanted))
	if a == "" || w == "" {
		return false
	}
	if a == w {
		return true
	}
	// A two-character minimum keeps a short name from matching half the index.
	if len([]rune(w)) < 2 {
		return false
	}
	return strings.Contains(a, w) || strings.Contains(w, a)
}

// decodeCursor revives the persisted cursor. A corrupt cursor is logged and
// ignored rather than failing the sync: the worst case is a re-sync of articles
// the knowledge base already has, which upserts in place.
func decodeCursor(ctx context.Context, sc *types.SyncCursor) *cursor {
	if sc == nil || sc.ConnectorCursor == nil {
		return nil
	}
	raw, err := json.Marshal(sc.ConnectorCursor)
	if err != nil {
		logger.Warnf(ctx, "[WeChat] marshal cursor: %v", err)
		return nil
	}
	var prev cursor
	if err := json.Unmarshal(raw, &prev); err != nil {
		logger.Warnf(ctx, "[WeChat] unmarshal cursor: %v", err)
		return nil
	}
	if prev.Seen == nil {
		prev.Seen = map[string]int64{}
	}
	return &prev
}

// toSyncCursor serializes the cursor for persistence.
func (c *cursor) toSyncCursor() *types.SyncCursor {
	raw, err := json.Marshal(c)
	if err != nil {
		return &types.SyncCursor{LastSyncTime: c.LastSyncTime}
	}
	var bag map[string]interface{}
	if err := json.Unmarshal(raw, &bag); err != nil {
		return &types.SyncCursor{LastSyncTime: c.LastSyncTime}
	}
	return &types.SyncCursor{LastSyncTime: c.LastSyncTime, ConnectorCursor: bag}
}

// publishedUnix returns the publish time as a unix timestamp, or now when the
// page did not carry one.
func publishedUnix(a article) int64 {
	if a.Published.IsZero() {
		return time.Now().Unix()
	}
	return a.Published.Unix()
}
