package wechat

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
)

// TestMain whitelists loopback so the httptest upstream (127.0.0.1) passes the
// SSRF guard every outbound request goes through.
func TestMain(m *testing.M) {
	_ = os.Setenv("SSRF_WHITELIST", "127.0.0.1,::1")
	requestInterval = 0
	os.Exit(m.Run())
}

// hit describes one result the fake upstream should render.
type hit struct {
	docID   string
	title   string
	account string
	pubUnix int64
	linkKey string
}

// fixture serves a fake Sogou WeChat search index: one page of hits per query,
// plus a /link hop that assembles the article URL in JavaScript pieces.
type fixture struct {
	mu       sync.Mutex
	pages    map[string][][]hit // query -> page-1-based index -> hits
	hops     int                // /link requests served
	searches int                // /weixin requests served
	blocked  bool
	redirect bool
	// markSession makes the upstream answer 200 but with a *live*
	// black_passportid cookie — the shape of a real blacklist notice (unlike
	// the deletion cookie every healthy response carries).
	markSession bool
	// lastQuery/lastPage record what the connector actually asked for.
	lastQuery     string
	lastPage      int
	lastHadWindow bool // ft/et present
	lastHadTSN    bool // tsn present — must never be true, see client.searchURL
	// cookies records the Cookie header of every /weixin request, so tests can
	// tell whether the jar kept the session (and the user's paste) alive.
	cookies []string
}

// pageOf builds the single-page listing most fixtures need.
func pageOf(hits ...hit) [][]hit { return [][]hit{hits} }

func newFixture(pages map[string][][]hit) *fixture {
	return &fixture{pages: pages}
}

func (f *fixture) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/weixin":
			f.serveSearch(w, r)
		case "/link":
			f.serveLink(w, r)
		default:
			http.NotFound(w, r)
		}
	})
}

func (f *fixture) serveSearch(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.searches++
	f.lastQuery = r.URL.Query().Get("query")
	f.lastPage, _ = strconv.Atoi(r.URL.Query().Get("page"))
	f.lastHadWindow = r.URL.Query().Get("ft") != ""
	f.lastHadTSN = r.URL.Query().Get("tsn") != ""
	f.cookies = append(f.cookies, r.Header.Get("Cookie"))
	blocked, redirect, markSession := f.blocked, f.redirect, f.markSession
	f.mu.Unlock()

	if redirect {
		// What the upstream does to a blacklisted client.
		w.Header().Set("Set-Cookie", "black_passportid=1; domain=.sogou.com; path=/")
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}
	if markSession {
		// 200 + a cookie that is still alive: this is a real blacklist notice.
		w.Header().Set("Set-Cookie", "black_passportid=1; domain=.sogou.com; path=/; max-age=86400")
		w.WriteHeader(http.StatusOK)
		return
	}
	if blocked {
		_, _ = w.Write([]byte(`<html><body><div id="seccodeVerify">请输入验证码</div></body></html>`))
		return
	}

	page := f.page(f.lastQuery, f.lastPage)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// 真实搜狗**每次正常响应**都会带 black_passportid，且 expires 设成 1994 年 ——
	// 语义是「清除这个标记」。fixture 必须照抄，否则「只看 cookie 名就判拦截」
	// 这个 bug 在本地永远测不出来（线上就是这么挂的）。
	//
	// SNUID 这里故意不带 domain：真实上游写的是 domain=.sogou.com，而 httptest
	// 跑在 127.0.0.1 上，带 domain 的 cookie 按规范会被 jar 拒收，那样就测不到
	// 「上游下发的会话 cookie 会在后续请求回传」这件事了。
	w.Header().Add("Set-Cookie", "SNUID=3657524A9593C82BFDFD65E596AFF3BE; path=/")
	w.Header().Add("Set-Cookie", "black_passportid=1; domain=.sogou.com; path=/; expires=Thu, 01-Dec-1994 16:00:00 GMT")
	if len(page) == 0 {
		_, _ = w.Write([]byte(`<html><body><div>抱歉，没有找到相关的微信公众号文章</div></body></html>`))
		return
	}
	_, _ = w.Write([]byte(renderResults(page)))
}

func (f *fixture) page(query string, page int) []hit {
	f.mu.Lock()
	defer f.mu.Unlock()
	byPage, ok := f.pages[query]
	if !ok || page < 1 || page > len(byPage) {
		return nil
	}
	return byPage[page-1]
}

func (f *fixture) serveLink(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.hops++
	f.mu.Unlock()
	key := r.URL.Query().Get("url")
	// The hop page hands back the target in three concatenated pieces, exactly
	// like the real one; nothing in it is a complete usable URL on its own.
	_, _ = fmt.Fprintf(w, `<html><head><script>
var url = '';
url += 'http://mp.weixin.qq.com/s?__biz=%s';
url += '&mid=2247&idx=1';
url += '&sn=%s';
</script></head><body></body></html>`, key, key)
}

// renderResults renders hits with the markup the parser expects.
func renderResults(hits []hit) string {
	var b strings.Builder
	b.WriteString(`<html><body><div class="news-box"><ul class="news-list">`)
	for i, h := range hits {
		fmt.Fprintf(&b, `<li id="sogou_vr_11002601_box_%d" d="%s">`, i, h.docID)
		b.WriteString(`<div class="txt-box"><h3><a target="_blank" href="/link?url=`)
		b.WriteString(url.QueryEscape(h.linkKey))
		b.WriteString(`&type=2&query=x" id="sogou_vr_11002601_title_` + strconv.Itoa(i) + `">`)
		b.WriteString(h.title)
		b.WriteString(`</a></h3><p class="txt-info" id="sogou_vr_11002601_summary_` + strconv.Itoa(i) + `">摘要</p>`)
		b.WriteString(`<div class="s-p"><span class="all-time-y2">`)
		b.WriteString(h.account)
		b.WriteString(`</span><span class="s2"><script>document.write(timeConvert('`)
		b.WriteString(strconv.FormatInt(h.pubUnix, 10))
		b.WriteString(`'))</script></span></div></div></li>`)
	}
	b.WriteString(`</ul></div></body></html>`)
	return b.String()
}

// configFor builds a DataSourceConfig the way the editor and the sync path do:
// non-secret values in Settings, cookie in Credentials.
func configFor(accounts, keywords []string, months, topN int) *types.DataSourceConfig {
	settings := map[string]interface{}{}
	if len(accounts) > 0 {
		settings["accounts"] = accounts
	}
	if len(keywords) > 0 {
		settings["keywords"] = keywords
	}
	if months > 0 {
		settings["months"] = months
	}
	if topN > 0 {
		settings["top_n"] = topN
	}
	return &types.DataSourceConfig{Type: types.ConnectorTypeWeChat, Settings: settings}
}

func daysAgo(n int) int64 {
	return time.Now().AddDate(0, 0, -n).Unix()
}

func TestParseSearchPage(t *testing.T) {
	body := renderResults([]hit{
		{docID: "d1", title: "标题一", account: "中国基金报", pubUnix: 1700000000, linkKey: "k1"},
		{docID: "d2", title: "标题二", account: "券商中国", pubUnix: 1700000100, linkKey: "k2"},
	})
	articles, err := parseSearchPage([]byte(body), "https://weixin.sogou.com")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(articles) != 2 {
		t.Fatalf("expected 2 articles, got %d", len(articles))
	}
	first := articles[0]
	if first.Title != "标题一" || first.Account != "中国基金报" || first.DocID != "d1" {
		t.Fatalf("unexpected first hit: %+v", first)
	}
	if !first.Published.Equal(time.Unix(1700000000, 0)) {
		t.Fatalf("published: got %v", first.Published)
	}
	if !strings.HasPrefix(first.Link, "https://weixin.sogou.com/link?url=") {
		t.Fatalf("link should be absolute, got %q", first.Link)
	}
}

func TestParseSearchPageEmptyAndBlocked(t *testing.T) {
	empty := `<html><body><div>抱歉，没有找到相关的微信公众号文章</div></body></html>`
	articles, err := parseSearchPage([]byte(empty), "https://weixin.sogou.com")
	if err != nil || articles != nil {
		t.Fatalf("empty page should yield no articles and no error, got %v / %v", articles, err)
	}

	if _, err := parseSearchPage([]byte(empty), "https://weixin.sogou.com/antispider/?from=x"); err == nil {
		// The path alone is not a signal; the body is. Assert the body path works.
		t.Log("path-only signal ignored, as intended")
	}

	blocked := `<html><body><div>您的访问过于频繁，请稍后再试</div></body></html>`
	if _, err := parseSearchPage([]byte(blocked), "https://weixin.sogou.com"); !errors.Is(err, ErrBlocked) {
		t.Fatalf("expected ErrBlocked, got %v", err)
	}

	junk := `<html><body><div>something else entirely</div></body></html>`
	if _, err := parseSearchPage([]byte(junk), "https://weixin.sogou.com"); !errors.Is(err, ErrUnexpectedPage) {
		t.Fatalf("expected ErrUnexpectedPage, got %v", err)
	}
}

func TestExtractJSURL(t *testing.T) {
	body := []byte(`<script>var url = ''; url += 'http://mp.weixin.qq.com/s?__biz=MzA5'; url += '&mid=1&amp;idx=1';</script>`)
	got := extractJSURL(body, "weixin.sogou.com")
	if got != "http://mp.weixin.qq.com/s?__biz=MzA5&mid=1&idx=1" {
		t.Fatalf("assembled URL wrong: %q", got)
	}

	// A single-assignment page must also work.
	body = []byte(`<script>url = 'https://mp.weixin.qq.com/s/abcdef';</script>`)
	if got := extractJSURL(body, "weixin.sogou.com"); got != "https://mp.weixin.qq.com/s/abcdef" {
		t.Fatalf("single assignment: %q", got)
	}

	// A non-article target must not be returned.
	body = []byte(`<script>url = 'https://weixin.sogou.com/';</script>`)
	if got := extractJSURL(body, "weixin.sogou.com"); got != "" {
		t.Fatalf("expected empty for non-article target, got %q", got)
	}
}

// 「上游地址」指向自建镜像时，文章页也挂在同一台机器上（本地联调就是
// http://127.0.0.1:8902/a/<key>）。这条必须认，否则镜像场景下每篇都会卡在
// “原文地址解析失败”，同步日志只写 partial，看起来像“一篇都没进来”。
func TestArticleURLFromUpstreamHost(t *testing.T) {
	body := []byte(`<script>var url = ''; url += 'http://127.0.0.1:8902/a/'; url += 'abc123';</script>`)
	got := extractJSURL(body, "127.0.0.1:8902")
	if got != "http://127.0.0.1:8902/a/abc123" {
		t.Fatalf("mirror article URL should be accepted: %q", got)
	}

	// 上游首页仍然不算文章页。
	home := []byte(`<script>url = 'http://127.0.0.1:8902/';</script>`)
	if got := extractJSURL(home, "127.0.0.1:8902"); got != "" {
		t.Fatalf("upstream home must not be treated as an article: %q", got)
	}

	// 第三种域名（既不是公众号也不是上游）仍然拒绝。
	other := []byte(`<script>url = 'https://example.com/tracking';</script>`)
	if got := extractJSURL(other, "127.0.0.1:8902"); got != "" {
		t.Fatalf("unrelated host must be rejected: %q", got)
	}
}

func TestFetchIncrementalDedupesAcrossSyncs(t *testing.T) {
	f := newFixture(map[string][][]hit{
		"中国基金报": pageOf(
			hit{docID: "d1", title: "私募清盘", account: "中国基金报", pubUnix: daysAgo(2), linkKey: "k1"},
			hit{docID: "d2", title: "ETF 扩容", account: "中国基金报", pubUnix: daysAgo(3), linkKey: "k2"},
			// Another account's article: the account subscription must filter it out.
			hit{docID: "d3", title: "别家文章", account: "券商中国", pubUnix: daysAgo(1), linkKey: "k3"},
		),
	})
	server := httptest.NewServer(f.handler())
	defer server.Close()

	cfg := configFor([]string{"中国基金报"}, nil, 3, 0)
	cfg.Settings["base_url"] = server.URL

	items, cur, err := NewConnector().FetchIncremental(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("first sync: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 items on first sync (other account filtered), got %d", len(items))
	}
	for _, item := range items {
		if !strings.HasPrefix(item.URL, "http://mp.weixin.qq.com/s?") {
			t.Fatalf("item URL should be the resolved article URL, got %q", item.URL)
		}
		if item.ExternalID == "" {
			t.Fatal("item must carry an external id")
		}
		if item.Metadata["channel"] != "wechat" {
			t.Fatalf("channel metadata: %v", item.Metadata)
		}
	}

	if f.hops != 2 {
		t.Fatalf("expected one hop per new article, got %d", f.hops)
	}

	// Second incremental run: everything already seen, no hops, no items.
	hopsBefore := f.hops
	items2, cur2, err := NewConnector().FetchIncremental(context.Background(), cfg, cur)
	if err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if len(items2) != 0 {
		t.Fatalf("second sync must be empty, got %d items", len(items2))
	}
	if f.hops != hopsBefore {
		t.Fatalf("second sync must not resolve already-seen articles (hops %d → %d)", hopsBefore, f.hops)
	}
	if cur2 == nil || cur2.ConnectorCursor == nil {
		t.Fatal("second sync must return a cursor")
	}
}

func TestKeywordTopNCapsCandidatesAndDedupesAcrossKeywords(t *testing.T) {
	shared := hit{docID: "d1", title: "同一篇", account: "中国基金报", pubUnix: daysAgo(1), linkKey: "k1"}
	f := newFixture(map[string][][]hit{
		"牛市": pageOf(shared, hit{docID: "d2", title: "第二篇", account: "券商中国", pubUnix: daysAgo(2), linkKey: "k2"}),
		"基金": pageOf(shared, hit{docID: "d3", title: "第三篇", account: "券商中国", pubUnix: daysAgo(3), linkKey: "k3"}),
	})
	server := httptest.NewServer(f.handler())
	defer server.Close()

	cfg := configFor(nil, []string{"牛市", "基金"}, 3, 1) // Y = 1 per keyword
	cfg.Settings["base_url"] = server.URL

	items, cur, err := NewConnector().FetchIncremental(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	// 两个关键词的 top-1 是同一篇：只应入库一次，也只解析一次跳转页。
	if len(items) != 1 || items[0].Title != "同一篇" {
		t.Fatalf("expected the shared top-1 only, got %+v", titles(items))
	}
	if f.hops != 1 {
		t.Fatalf("expected 1 hop, got %d", f.hops)
	}

	// ⚠️ Y 是对**候选**的截断，不是对"新增"的截断：第二轮必须还是 0 篇。
	// 旧实现把已入库的先滤掉再数 Y，于是每轮都会往后挪一格，把结果集慢慢吞完。
	hopsBefore := f.hops
	items2, _, err := NewConnector().FetchIncremental(context.Background(), cfg, cur)
	if err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if len(items2) != 0 {
		t.Fatalf("Y must not slide with the cursor, got %+v", titles(items2))
	}
	if f.hops != hopsBefore {
		t.Fatalf("second sync must not resolve anything (hops %d → %d)", hopsBefore, f.hops)
	}
}

func TestKeywordTopNTakesNewestWhenRoomLeft(t *testing.T) {
	// Y = 2：每个关键词各贡献 2 篇（共 4 篇，账号不同、标题不同）。
	f := newFixture(map[string][][]hit{
		"牛市": pageOf(
			hit{docID: "a1", title: "牛一", account: "甲号", pubUnix: daysAgo(1), linkKey: "k1"},
			hit{docID: "a2", title: "牛二", account: "乙号", pubUnix: daysAgo(2), linkKey: "k2"},
			hit{docID: "a3", title: "牛三", account: "丙号", pubUnix: daysAgo(3), linkKey: "k3"},
		),
		"基金": pageOf(
			hit{docID: "b1", title: "基一", account: "甲号", pubUnix: daysAgo(1), linkKey: "k4"},
			hit{docID: "b2", title: "基二", account: "乙号", pubUnix: daysAgo(2), linkKey: "k5"},
			hit{docID: "b3", title: "基三", account: "丙号", pubUnix: daysAgo(3), linkKey: "k6"},
		),
	})
	server := httptest.NewServer(f.handler())
	defer server.Close()

	cfg := configFor(nil, []string{"牛市", "基金"}, 3, 2)
	cfg.Settings["base_url"] = server.URL

	items, _, err := NewConnector().FetchIncremental(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	got := titles(items)
	if len(got) != 4 {
		t.Fatalf("expected 2 per keyword = 4, got %+v", got)
	}
	if slices.Contains(got, "牛三") || slices.Contains(got, "基三") {
		t.Fatalf("Y=2 must drop the third hit, got %+v", got)
	}
}

func titles(items []types.FetchedItem) []string {
	out := make([]string, 0, len(items))
	for _, i := range items {
		out = append(out, i.Title)
	}
	return out
}

func TestWindowFilterDropsOldArticles(t *testing.T) {
	f := newFixture(map[string][][]hit{
		"基金": pageOf(
			hit{docID: "old", title: "四个月前", account: "券商中国", pubUnix: daysAgo(120), linkKey: "kold"},
			hit{docID: "new", title: "上周", account: "券商中国", pubUnix: daysAgo(7), linkKey: "knew"},
		),
	})
	server := httptest.NewServer(f.handler())
	defer server.Close()

	cfg := configFor(nil, []string{"基金"}, 3, 0)
	cfg.Settings["base_url"] = server.URL

	items, _, err := NewConnector().FetchIncremental(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if len(items) != 1 || items[0].Title != "上周" {
		t.Fatalf("expected only the in-window article, got %+v", titles(items))
	}
}

func TestPaginationStopsOnEmptyPage(t *testing.T) {
	f := newFixture(map[string][][]hit{
		"基金": {
			{hit{docID: "d1", title: "第一篇", account: "券商中国", pubUnix: daysAgo(1), linkKey: "k1"}},
			nil, // page 2 empty → stop
		},
	})
	server := httptest.NewServer(f.handler())
	defer server.Close()

	cfg := configFor(nil, []string{"基金"}, 3, 0)
	cfg.Settings["base_url"] = server.URL

	items, _, err := NewConnector().FetchIncremental(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if f.searches != 2 {
		t.Fatalf("expected 2 search requests (page 1 + empty page 2), got %d", f.searches)
	}
}

func TestValidateReportsBlockedUpstream(t *testing.T) {
	f := newFixture(nil)
	f.redirect = true
	server := httptest.NewServer(f.handler())
	defer server.Close()

	cfg := configFor(nil, []string{"基金"}, 3, 0)
	cfg.Settings["base_url"] = server.URL
	err := NewConnector().Validate(context.Background(), cfg)
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("expected ErrBlocked from validate, got %v", err)
	}
	if !strings.Contains(err.Error(), "Cookie") {
		t.Fatalf("validate error should tell the user about the Cookie escape hatch, got %q", err.Error())
	}

	f2 := newFixture(nil)
	f2.blocked = true
	server2 := httptest.NewServer(f2.handler())
	defer server2.Close()
	cfg2 := configFor(nil, []string{"基金"}, 3, 0)
	cfg2.Settings["base_url"] = server2.URL
	if err := NewConnector().Validate(context.Background(), cfg2); !errors.Is(err, ErrBlocked) {
		t.Fatalf("expected ErrBlocked for the interstitial body, got %v", err)
	}
}

func TestValidateSendsTimeWindowWithoutTSN(t *testing.T) {
	f := newFixture(map[string][][]hit{"基金": pageOf(hit{docID: "d1", title: "t", account: "a", pubUnix: daysAgo(1), linkKey: "k"})})
	server := httptest.NewServer(f.handler())
	defer server.Close()

	cfg := configFor(nil, []string{"基金"}, 3, 0)
	cfg.Settings["base_url"] = server.URL
	if err := NewConnector().Validate(context.Background(), cfg); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if !f.lastHadWindow {
		t.Fatal("expected ft/et on the request")
	}
	// ⚠️ tsn=5 会让搜狗对**任何**请求返回 302（线上「测试连接」全挂的元凶）。
	// 这条断言是防回归的唯一守卫，别为了让上游「收窄范围」再把它加回来。
	if f.lastHadTSN {
		t.Fatal("request must not carry tsn: sogou answers 302 to any request with tsn=5")
	}
}

// 正常 200 响应里带的 black_passportid 是「删除该标记」（expires 在过去），
// 不能当成拦截。这个误判曾经让每一次正常同步都报「会话被标记」。
func TestDeletionCookieOnHealthyResponseIsNotBlocked(t *testing.T) {
	f := newFixture(map[string][][]hit{"基金": pageOf(hit{docID: "d1", title: "正常文章", account: "甲号", pubUnix: daysAgo(1), linkKey: "k1"})})
	server := httptest.NewServer(f.handler())
	defer server.Close()

	cfg := configFor(nil, []string{"基金"}, 3, 0)
	cfg.Settings["base_url"] = server.URL

	items, _, err := NewConnector().FetchIncremental(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("healthy response must not be reported as blocked: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected the article, got %+v", titles(items))
	}
}

// 真正生效中的 black_passportid（max-age 未过期）仍然要判拦截。
func TestLiveBlacklistCookieIsBlocked(t *testing.T) {
	f := newFixture(map[string][][]hit{"基金": pageOf(hit{docID: "d1", title: "x", account: "a", pubUnix: daysAgo(1), linkKey: "k"})})
	f.markSession = true
	server := httptest.NewServer(f.handler())
	defer server.Close()

	cfg := configFor(nil, []string{"基金"}, 3, 0)
	cfg.Settings["base_url"] = server.URL
	if err := NewConnector().Validate(context.Background(), cfg); !errors.Is(err, ErrBlocked) {
		t.Fatalf("expected ErrBlocked for a live black_passportid, got %v", err)
	}
}

// 用户手填的 Cookie 必须真的发出去（以前手工写 Cookie 头，后来改成塞 jar，
// 这个用例守住两者都没退化）；同时上游下发的会话 cookie 要在后续请求里回传，
// 否则每一页都是新匿名访客，翻两页就被拦。
func TestCookieJarKeepsUserCookieAndSession(t *testing.T) {
	f := newFixture(map[string][][]hit{
		"基金": {
			{hit{docID: "d1", title: "第一篇", account: "甲号", pubUnix: daysAgo(1), linkKey: "k1"}},
			{hit{docID: "d2", title: "第二篇", account: "甲号", pubUnix: daysAgo(2), linkKey: "k2"}},
		},
	})
	server := httptest.NewServer(f.handler())
	defer server.Close()

	cfg := configFor(nil, []string{"基金"}, 3, 0)
	cfg.Settings["base_url"] = server.URL
	// Cookie 是唯一的加密凭据，只能走 Credentials。
	cfg.Credentials = map[string]interface{}{"cookie": "SNUID=usercookie123; SUID=usersuid"}

	if err := NewConnector().Validate(context.Background(), cfg); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if len(f.cookies) == 0 || !strings.Contains(f.cookies[0], "SNUID=usercookie123") {
		t.Fatalf("user cookie must be sent, got %q", f.cookies)
	}

	f.mu.Lock()
	f.cookies = nil
	f.mu.Unlock()
	if _, _, err := NewConnector().FetchIncremental(context.Background(), cfg, nil); err != nil {
		t.Fatalf("sync: %v", err)
	}

	f.mu.Lock()
	got := strings.Join(f.cookies, " || ")
	f.mu.Unlock()
	// 上游第二页起应当看到它自己下发的 SNUID（fixture 的健康响应里带的那个）。
	if !strings.Contains(got, "3657524A9593C82BFDFD65E596AFF3BE") {
		t.Fatalf("session cookie from the upstream must be echoed on later requests, got %q", got)
	}
}

func TestListResourcesMirrorsConfiguration(t *testing.T) {
	cfg := configFor([]string{"中国基金报"}, []string{"牛市"}, 3, 20)
	resources, err := NewConnector().ListResources(context.Background(), cfg, "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(resources) != 2 {
		t.Fatalf("expected 2 resources, got %d", len(resources))
	}
	if resources[0].ExternalID != ResourcePrefixAccount+"中国基金报" || resources[0].Type != "account" {
		t.Fatalf("account resource wrong: %+v", resources[0])
	}
	if resources[1].ExternalID != ResourcePrefixKeyword+"牛市" || resources[1].Type != "keyword" {
		t.Fatalf("keyword resource wrong: %+v", resources[1])
	}
	if !strings.Contains(resources[1].Description, "20") {
		t.Fatalf("keyword description should mention the cap: %q", resources[1].Description)
	}

	// A lazy-load request for a parent has nothing extra to list.
	if extra, _ := NewConnector().ListResources(context.Background(), cfg, "account:中国基金报"); len(extra) != 0 {
		t.Fatalf("expected no children, got %d", len(extra))
	}
}

func TestConfigParsing(t *testing.T) {
	// Settings win over credentials; credentials-only (validate-credentials) works.
	cfg := &types.DataSourceConfig{
		Type: types.ConnectorTypeWeChat,
		Credentials: map[string]interface{}{
			"accounts": []interface{}{"甲", "乙"},
			"keywords": "牛市\n基金",
			"months":   float64(6),
			"top_n":    float64(10),
			"cookie":   "SNUID=abc",
		},
		Settings: map[string]interface{}{
			"accounts": []interface{}{"丙"},
			"months":   float64(2),
			"top_n":    float64(5),
		},
	}
	parsed, err := parseConfig(cfg)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(parsed.Accounts) != 1 || parsed.Accounts[0] != "丙" {
		t.Fatalf("settings must win for accounts: %v", parsed.Accounts)
	}
	if len(parsed.Keywords) != 2 || parsed.Keywords[0] != "牛市" {
		t.Fatalf("keywords from credentials must survive: %v", parsed.Keywords)
	}
	if parsed.Months != 2 || parsed.TopN != 5 {
		t.Fatalf("windows: months=%d topN=%d", parsed.Months, parsed.TopN)
	}
	if parsed.Cookie != "SNUID=abc" {
		t.Fatalf("cookie: %q", parsed.Cookie)
	}

	// Defaults.
	parsed, err = parseConfig(configFor(nil, []string{"基金"}, 0, 0))
	if err != nil {
		t.Fatalf("defaults: %v", err)
	}
	if parsed.Months != DefaultMonths || parsed.TopN != DefaultTopN {
		t.Fatalf("defaults: months=%d topN=%d", parsed.Months, parsed.TopN)
	}
	if parsed.GetBaseURL() != DefaultBaseURL {
		t.Fatalf("base url default: %q", parsed.GetBaseURL())
	}

	// Bounds and the empty-config case.
	if _, err := parseConfig(configFor(nil, nil, 0, 0)); !errors.Is(err, datasource.ErrInvalidConfig) {
		t.Fatalf("empty config must be rejected, got %v", err)
	}
	if _, err := parseConfig(configFor(nil, []string{"基金"}, MaxMonths+1, 0)); !errors.Is(err, datasource.ErrInvalidConfig) {
		t.Fatalf("months over the cap must be rejected, got %v", err)
	}
	if _, err := parseConfig(configFor(nil, []string{"基金"}, 1, MaxTopN+1)); !errors.Is(err, datasource.ErrInvalidConfig) {
		t.Fatalf("top_n over the cap must be rejected, got %v", err)
	}

	// Dedupe + trim of a comma separated list, including the full-width comma.
	cfg = configFor(nil, nil, 1, 1)
	cfg.Settings["accounts"] = "甲, 乙，甲\n"
	parsed, err = parseConfig(cfg)
	if err != nil {
		t.Fatalf("list parse: %v", err)
	}
	if len(parsed.Accounts) != 2 {
		t.Fatalf("expected 2 deduped accounts, got %v", parsed.Accounts)
	}
}

// 回归：**配置是同步范围的唯一依据**。
//
// 线上事故形态：用户在编辑数据源时把公众号从「甲号」改成「甲号 + 乙号」，但
// WeKnora 里存的 resource_ids 还是上一次保存时的镜像（只有 account:甲号）。
// 旧实现按 resource_ids 收窄——因为还有一项对得上，收窄"成功"了，于是乙号
// 永远同步不到，而同步日志照样写 success。用户看到的就是"配好了却一篇没进来"。
func TestSubscriptionConfigDrivesTheSyncScope(t *testing.T) {
	f := newFixture(map[string][][]hit{
		"甲号": pageOf(hit{docID: "d1", title: "甲文", account: "甲号", pubUnix: daysAgo(1), linkKey: "k1"}),
		"乙号": pageOf(hit{docID: "d2", title: "乙文", account: "乙号", pubUnix: daysAgo(1), linkKey: "k2"}),
	})
	server := httptest.NewServer(f.handler())
	defer server.Close()

	cfg := configFor([]string{"甲号", "乙号"}, nil, 3, 0)
	cfg.Settings["base_url"] = server.URL
	// 上一次保存的选择只覆盖了甲号 —— 乙号是后来加的。
	cfg.ResourceIDs = []string{ResourcePrefixAccount + "甲号"}

	items, _, err := NewConnector().FetchIncremental(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if got := titles(items); len(got) != 2 {
		t.Fatalf("newly added subscription must be synced too, got %+v", got)
	}

	// 选择整体失效（订阅改名）同样不能把同步卡死。
	cfg.ResourceIDs = []string{ResourcePrefixAccount + "已改名"}
	items, _, err = NewConnector().FetchIncremental(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("sync with stale selection: %v", err)
	}
	if got := titles(items); len(got) != 2 {
		t.Fatalf("stale selection must not narrow the sync, got %+v", got)
	}
}

func TestCursorTrimsOldestEntries(t *testing.T) {
	c := &cursor{Seen: make(map[string]int64, maxSeenEntries+5)}
	for i := 0; i < maxSeenEntries+5; i++ {
		c.Seen[fmt.Sprintf("id-%d", i)] = int64(i)
	}
	c.trim()
	if len(c.Seen) != maxSeenEntries {
		t.Fatalf("trim: expected %d entries, got %d", maxSeenEntries, len(c.Seen))
	}
	if _, kept := c.Seen["id-0"]; kept {
		t.Fatal("trim must drop the oldest entries first")
	}
	if _, kept := c.Seen[fmt.Sprintf("id-%d", maxSeenEntries+4)]; !kept {
		t.Fatal("trim must keep the newest entries")
	}
}

func TestArticleExternalIDIsStableAndUnique(t *testing.T) {
	a := article{Title: "标题", Account: "中国基金报", Published: time.Unix(1700000000, 0), DocID: "d1"}
	b := article{Title: "标题", Account: "中国基金报", Published: time.Unix(1700000000, 0), DocID: "d1"}
	if a.externalID() != b.externalID() {
		t.Fatal("identity must be deterministic")
	}
	other := article{Title: "标题", Account: "券商中国", Published: time.Unix(1700000000, 0)}
	if a.externalID() == other.externalID() {
		t.Fatal("different accounts must not collide")
	}
	// No publish time → upstream doc id keeps the identity stable.
	undated := article{Title: "标题", Account: "中国基金报", DocID: "d9"}
	if !strings.HasPrefix(undated.externalID(), "wechat:doc:d9") {
		t.Fatalf("undated identity should use the doc id, got %q", undated.externalID())
	}
}
