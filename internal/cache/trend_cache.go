package cache

import (
	"fmt"
	"strings"
	"time"

	"bilibili-up-admin/internal/model"
	"bilibili-up-admin/pkg/bilibili"

	gocache "github.com/patrickmn/go-cache"
)

type CacheStatus string

const (
	CacheFresh CacheStatus = "fresh"
	CacheStale CacheStatus = "stale"
	CacheMiss  CacheStatus = "miss"
)

type TrendCache struct {
	store    *gocache.Cache
	ttl      time.Duration
	staleTTL time.Duration
	logf     func(format string, args ...any)
}

type trendingTagsEntry struct {
	Tags       []bilibili.TrendingTag
	CachedAt   time.Time
	FreshUntil time.Time
	Source     string
}

type rankingsEntry struct {
	Rankings   []model.TagRanking
	CachedAt   time.Time
	FreshUntil time.Time
	Source     string
}

func NewTrendCache(ttl, staleTTL time.Duration, logf func(format string, args ...any)) *TrendCache {
	if ttl <= 0 {
		ttl = time.Minute
	}
	if staleTTL < ttl {
		staleTTL = ttl
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &TrendCache{
		store:    gocache.New(staleTTL, 10*time.Minute),
		ttl:      ttl,
		staleTTL: staleTTL,
		logf:     logf,
	}
}

func (c *TrendCache) GetTrendingTags(category string, limit int) ([]bilibili.TrendingTag, CacheStatus, bool) {
	if c == nil || c.store == nil {
		return nil, CacheMiss, false
	}
	key := trendTagsKey(category)
	value, ok := c.store.Get(key)
	if !ok {
		c.logf("[trend.cache] miss type=tags key=%s category=%q limit=%d", key, category, limit)
		return nil, CacheMiss, false
	}
	entry, ok := value.(trendingTagsEntry)
	if !ok || len(entry.Tags) == 0 {
		c.logf("[trend.cache] miss type=tags key=%s category=%q limit=%d reason=invalid_entry", key, category, limit)
		return nil, CacheMiss, false
	}
	status := CacheFresh
	if time.Now().After(entry.FreshUntil) {
		status = CacheStale
	}
	tags := limitTrendingTags(entry.Tags, limit)
	c.logf("[trend.cache] %s type=tags key=%s category=%q limit=%d count=%d source=%s age=%s", status, key, category, limit, len(tags), entry.Source, time.Since(entry.CachedAt).Round(time.Second))
	return tags, status, true
}

func (c *TrendCache) SetTrendingTags(category string, tags []bilibili.TrendingTag, ttl time.Duration, source string) {
	if c == nil || c.store == nil || len(tags) == 0 {
		return
	}
	if ttl <= 0 {
		ttl = c.ttl
	}
	staleTTL := c.staleTTL
	if staleTTL < ttl {
		staleTTL = ttl
	}
	key := trendTagsKey(category)
	now := time.Now()
	c.store.Set(key, trendingTagsEntry{
		Tags:       cloneTrendingTags(tags),
		CachedAt:   now,
		FreshUntil: now.Add(ttl),
		Source:     source,
	}, staleTTL)
	c.logf("[trend.cache] set type=tags key=%s category=%q count=%d ttl=%s stale_ttl=%s source=%s", key, category, len(tags), ttl, staleTTL, source)
}

func (c *TrendCache) GetRankings(category string, limit int) ([]model.TagRanking, CacheStatus, bool) {
	if c == nil || c.store == nil {
		return nil, CacheMiss, false
	}
	key := rankingsKey(category)
	value, ok := c.store.Get(key)
	if !ok {
		c.logf("[trend.cache] miss type=rankings key=%s category=%q limit=%d", key, category, limit)
		return nil, CacheMiss, false
	}
	entry, ok := value.(rankingsEntry)
	if !ok || len(entry.Rankings) == 0 {
		c.logf("[trend.cache] miss type=rankings key=%s category=%q limit=%d reason=invalid_entry", key, category, limit)
		return nil, CacheMiss, false
	}
	status := CacheFresh
	if time.Now().After(entry.FreshUntil) {
		status = CacheStale
	}
	rankings := limitRankings(entry.Rankings, limit)
	c.logf("[trend.cache] %s type=rankings key=%s category=%q limit=%d count=%d source=%s age=%s", status, key, category, limit, len(rankings), entry.Source, time.Since(entry.CachedAt).Round(time.Second))
	return rankings, status, true
}

func (c *TrendCache) SetRankings(category string, rankings []model.TagRanking, ttl time.Duration, source string) {
	if c == nil || c.store == nil || len(rankings) == 0 {
		return
	}
	if ttl <= 0 {
		ttl = c.ttl
	}
	staleTTL := c.staleTTL
	if staleTTL < ttl {
		staleTTL = ttl
	}
	key := rankingsKey(category)
	now := time.Now()
	c.store.Set(key, rankingsEntry{
		Rankings:   cloneRankings(rankings),
		CachedAt:   now,
		FreshUntil: now.Add(ttl),
		Source:     source,
	}, staleTTL)
	c.logf("[trend.cache] set type=rankings key=%s category=%q count=%d ttl=%s stale_ttl=%s source=%s", key, category, len(rankings), ttl, staleTTL, source)
}

func trendTagsKey(category string) string {
	return fmt.Sprintf("trend:tags:%s", normalizeCachePart(category))
}

func rankingsKey(category string) string {
	return fmt.Sprintf("trend:rankings:%s", normalizeCachePart(category))
}

func normalizeCachePart(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "all"
	}
	return strings.ToLower(value)
}

func limitTrendingTags(tags []bilibili.TrendingTag, limit int) []bilibili.TrendingTag {
	if limit > 0 && limit < len(tags) {
		tags = tags[:limit]
	}
	return cloneTrendingTags(tags)
}

func limitRankings(rankings []model.TagRanking, limit int) []model.TagRanking {
	if limit > 0 && limit < len(rankings) {
		rankings = rankings[:limit]
	}
	return cloneRankings(rankings)
}

func cloneTrendingTags(tags []bilibili.TrendingTag) []bilibili.TrendingTag {
	out := make([]bilibili.TrendingTag, len(tags))
	copy(out, tags)
	return out
}

func cloneRankings(rankings []model.TagRanking) []model.TagRanking {
	out := make([]model.TagRanking, len(rankings))
	copy(out, rankings)
	return out
}
