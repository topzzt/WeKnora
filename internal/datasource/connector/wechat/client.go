package wechat

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/Tencent/WeKnora/internal/utils"
)

const (
	// requestTimeout bounds a single upstream request.
	requestTimeout = 20 * time.Second

	// maxPageSize caps a results page body.
	maxPageSize = 4 * 1024 * 1024

	// defaultUserAgent is sent on every request. The upstream answers a bare
	// client (curl, Go-http-client) with an empty page, so a browser UA is not
	// optional here.
	defaultUserAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) " +
		"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"

	// minRequestInterval throttles consecutive upstream requests. The index is
	// aggressively rate limited; bursts of parallel or back-to-back requests get
	// the IP blacklisted for minutes, which is far more expensive than waiting.
	minRequestInterval = 1200 * time.Millisecond

	// maxPagesPerTarget bounds how deep a single subscription paginates. Ten
	// pages is the upstream's practical ceiling and already 100 candidate hits.
	maxPagesPerTarget = 10
)

// requestInterval is the production throttle; tests shorten it so the suite does
// not spend a second per upstream call.
var requestInterval = minRequestInterval

// ErrBlocked reports that the upstream answered with its anti-spider
// interstitial (or a redirect to it) instead of a results page. It is a
// permanent-looking but transient condition: the same request usually succeeds
// minutes later, or immediately with a browser Cookie supplied.
var ErrBlocked = errors.New("搜狗微信搜索拦截了本次请求（需要验证码或访问过于频繁）")

// ErrUnexpectedPage reports a response that is neither a results page nor a
// well-formed "no results" page. It usually means the upstream changed its
// markup or a proxy in between rewrote the response.
var ErrUnexpectedPage = errors.New("上游返回了无法识别的页面")

// client performs SSRF-safe, throttled reads against the upstream search index.
type client struct {
	http *http.Client
	base string
	// host is the upstream host (sogou in production). Article URLs are also
	// accepted from this host so a self-hosted mirror can be tested end to end.
	host      string
	cookie    string
	userAgent string

	mu   sync.Mutex
	last time.Time
	// interval is minRequestInterval in production; tests shorten it.
	interval time.Duration
}

func newClient(cfg *Config) *client {
	httpCfg := utils.DefaultSSRFSafeHTTPClientConfig()
	httpCfg.Timeout = requestTimeout
	base := cfg.GetBaseURL()
	host := ""
	if u, err := url.Parse(base); err == nil {
		host = u.Host
	}
	return &client{
		http:      utils.NewSSRFSafeHTTPClient(httpCfg),
		base:      base,
		host:      host,
		cookie:    strings.TrimSpace(cfg.Cookie),
		userAgent: defaultUserAgent,
		interval:  requestInterval,
	}
}

// wait blocks until the next request is allowed. Requests are serialized: the
// connector syncs one page at a time anyway, and a shared client keeps the
// throttle honest across subscriptions.
func (c *client) wait(ctx context.Context) error {
	c.mu.Lock()
	delay := time.Duration(0)
	if !c.last.IsZero() {
		if elapsed := time.Since(c.last); elapsed < c.interval {
			delay = c.interval - elapsed
		}
	}
	c.last = time.Now().Add(delay)
	c.mu.Unlock()

	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// get fetches rawURL and returns the body. Redirects are NOT followed: the
// upstream signals a blacklisted client with a 302 to its home page, and
// following it would turn that into an indistinguishable "page without results".
func (c *client) get(ctx context.Context, rawURL, referer string) ([]byte, error) {
	if err := utils.ValidateURLForSSRF(rawURL); err != nil {
		return nil, fmt.Errorf("URL rejected: %w", err)
	}
	if err := c.wait(ctx); err != nil {
		return nil, err
	}

	reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
	if referer != "" {
		req.Header.Set("Referer", referer)
	}
	if c.cookie != "" {
		req.Header.Set("Cookie", c.cookie)
	}

	noRedirect := *c.http
	noRedirect.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}

	resp, err := noRedirect.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request %s: %w", rawURL, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxPageSize))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return nil, fmt.Errorf("%w（HTTP %d 跳转；可稍后重试或填写 Cookie）", ErrBlocked, resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w（HTTP %d）", ErrUnexpectedPage, resp.StatusCode)
	}
	// Sogou cancels the session by setting black_passportid when it blacklists.
	for _, ck := range resp.Cookies() {
		if ck.Name == "black_passportid" {
			return nil, fmt.Errorf("%w（会话被标记）", ErrBlocked)
		}
	}
	return body, nil
}

// search fetches one results page for query. When the time window parameters are
// rejected (the upstream occasionally answers a challenge page for custom
// ranges), it retries once without them and lets the caller filter by publish
// time instead.
func (c *client) search(ctx context.Context, query string, page int, since, until time.Time) ([]article, error) {
	withWindow := c.searchURL(query, page, since, until)
	body, err := c.get(ctx, withWindow, c.base+"/")
	if err != nil {
		return nil, err
	}
	articles, err := parseSearchPage(body, c.base)
	if err == nil {
		return articles, nil
	}
	if !errors.Is(err, ErrUnexpectedPage) || since.IsZero() {
		return nil, err
	}

	// Retry without tsn/ft/et: a page that parses fine without the window is
	// still usable because walk() filters every article by publish time.
	plainURL := c.searchURL(query, page, time.Time{}, time.Time{})
	body, plainErr := c.get(ctx, plainURL, c.base+"/")
	if plainErr != nil {
		return nil, err
	}
	return parseSearchPage(body, c.base)
}

// searchURL builds one article-search URL. tsn=5 is the upstream's "custom
// range" mode, taking ft (from) and et (to) as YYYY-MM-DD.
func (c *client) searchURL(query string, page int, since, until time.Time) string {
	u, err := url.Parse(c.base + "/weixin")
	if err != nil {
		return c.base + "/weixin"
	}
	q := u.Query()
	q.Set("type", "2")
	q.Set("query", query)
	q.Set("ie", "utf8")
	q.Set("s_from", "input")
	if !since.IsZero() {
		q.Set("tsn", "5")
		q.Set("ft", since.Format("2006-01-02"))
		q.Set("et", until.Format("2006-01-02"))
	}
	q.Set("page", strconv.Itoa(page))
	u.RawQuery = q.Encode()
	return u.String()
}

// resolveLink turns an upstream /link?url=… hop into the real mp.weixin.qq.com
// article URL. The hop's body is JavaScript that concatenates the target in
// pieces, so the pieces are joined in order rather than regex-matched for one
// complete URL.
func (c *client) resolveLink(ctx context.Context, link string) (string, error) {
	body, err := c.get(ctx, link, c.base+"/")
	if err != nil {
		return "", err
	}
	target := extractJSURL(body, c.host)
	if target == "" {
		if isBlockedBody(body) {
			return "", ErrBlocked
		}
		return "", fmt.Errorf("%w：未能从跳转页解析出文章地址", ErrUnexpectedPage)
	}
	return target, nil
}

var (
	// jsURLAssign matches both `url = '…'` and `url += '…'` assignments.
	jsURLAssign = regexp.MustCompile(`url\s*(\+?)=\s*'([^']*)'`)
	// jsLocation matches a direct `location.href = "http…"` redirect.
	jsLocation = regexp.MustCompile(`location\.(?:href|replace\()\s*=?\s*["'](https?://[^"']+)["']`)
)

// extractJSURL concatenates the URL pieces of a hop page and returns the first
// assembled URL that points at the article host.
func extractJSURL(body []byte, upstreamHost string) string {
	var sb strings.Builder
	started := false
	for _, m := range jsURLAssign.FindAllSubmatch(body, -1) {
		op := string(m[1])
		piece := string(m[2])
		if op == "" {
			// A plain assignment resets the accumulator; the page emits an empty
			// `url = ''` before appending, and a real URL here means the whole
			// target is in one piece.
			sb.Reset()
			started = strings.HasPrefix(piece, "http")
			sb.WriteString(piece)
			continue
		}
		// `url += '…'` continues the accumulator. A page that starts with append
		// (no empty reset first) still has to be assembled, so a piece that looks
		// like the head of a URL starts it.
		if !started {
			if !strings.HasPrefix(piece, "http") {
				continue
			}
			started = true
			sb.Reset()
		}
		sb.WriteString(piece)
	}
	candidate := strings.ReplaceAll(sb.String(), "&amp;", "&")
	if isArticleURL(candidate, upstreamHost) {
		return candidate
	}
	for _, m := range jsLocation.FindAllSubmatch(body, -1) {
		candidate := strings.ReplaceAll(string(m[1]), "&amp;", "&")
		if isArticleURL(candidate, upstreamHost) {
			return candidate
		}
	}
	return ""
}

// isArticleURL reports whether rawURL addresses an article page.
//
// 生产上游（搜狗）的跳转页只会拼出 mp.weixin.qq.com/s… 的地址，这是主判据。
// 第二个分支放宽到"上游自己的域名"：把「上游地址」指向自建镜像时，文章页也挂在
// 同一台机器上（本地联调就是 http://127.0.0.1:8902/a/xxx）。没有这条分支，
// 镜像场景下每篇都会卡在"原文地址解析失败"，而日志里只有一句干巴巴的失败。
func isArticleURL(rawURL, upstreamHost string) bool {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || u.Host == "" {
		return false
	}
	if u.Host == "mp.weixin.qq.com" {
		return strings.HasPrefix(u.Path, "/s")
	}
	if upstreamHost != "" && strings.EqualFold(u.Host, upstreamHost) {
		return u.Path != "" && u.Path != "/"
	}
	return false
}

// parseSearchPage extracts the hits of one results page. A page with the
// upstream's "nothing found" copy is a valid empty result; anything else without
// hits means the markup changed or a proxy answered for the upstream.
func parseSearchPage(body []byte, base string) ([]article, error) {
	if isBlockedBody(body) {
		return nil, ErrBlocked
	}
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("%w：%v", ErrUnexpectedPage, err)
	}

	out := make([]article, 0, 10)
	doc.Find("ul.news-list li").Each(func(_ int, sel *goquery.Selection) {
		titleSel := sel.Find("h3 a").First()
		href, ok := titleSel.Attr("href")
		title := strings.TrimSpace(titleSel.Text())
		if !ok || title == "" {
			return
		}
		docID, _ := sel.Attr("d")
		out = append(out, article{
			DocID:     strings.TrimSpace(docID),
			Title:     title,
			Summary:   strings.TrimSpace(sel.Find("p.txt-info").First().Text()),
			Account:   strings.TrimSpace(sel.Find("span.all-time-y2").First().Text()),
			Link:      absoluteURL(base, href),
			Published: parsePublished(sel),
		})
	})
	if len(out) > 0 {
		return out, nil
	}

	text := doc.Text()
	if strings.Contains(text, "没有找到") || strings.Contains(text, "暂无") {
		return nil, nil
	}
	return nil, ErrUnexpectedPage
}

// parsePublished reads the publish time of one hit. The upstream renders it as
// `document.write(timeConvert('1452866931'))`; a plain date string is accepted as
// a fallback for older markup.
func parsePublished(sel *goquery.Selection) time.Time {
	span := sel.Find("span.s2").First()
	script := span.Find("script")
	raw := span.Text()
	if script.Length() > 0 {
		raw = script.Text()
	}
	if m := timeConvert.FindStringSubmatch(raw); len(m) == 2 {
		if ts, err := strconv.ParseInt(m[1], 10, 64); err == nil {
			return time.Unix(ts, 0)
		}
	}
	flat := strings.TrimSpace(raw)
	for _, layout := range []string{"2006-01-02", "2006年01月02日", "2006/01/02"} {
		if t, err := time.ParseInLocation(layout, flat, time.Local); err == nil {
			return t
		}
	}
	return time.Time{}
}

var timeConvert = regexp.MustCompile(`timeConvert\('(\d+)'\)`)

// blockedMarkers are the phrases the anti-spider interstitial and the
// blacklisted-session page contain.
var blockedMarkers = []string{
	"antispider",
	"请输入验证码",
	"访问过于频繁",
	"您的访问出错了",
	"black_passportid",
}

func isBlockedBody(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	lower := bytes.ToLower(body)
	for _, marker := range blockedMarkers {
		if bytes.Contains(lower, []byte(strings.ToLower(marker))) {
			return true
		}
	}
	return false
}

// absoluteURL resolves a possibly relative href against the upstream host.
func absoluteURL(base, href string) string {
	href = strings.TrimSpace(href)
	if href == "" {
		return ""
	}
	u, err := url.Parse(href)
	if err != nil {
		return href
	}
	if u.IsAbs() {
		return u.String()
	}
	b, err := url.Parse(base)
	if err != nil {
		return href
	}
	return b.ResolveReference(u).String()
}
