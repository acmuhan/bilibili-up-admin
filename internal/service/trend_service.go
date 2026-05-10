package service

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	appcache "bilibili-up-admin/internal/cache"
	"bilibili-up-admin/internal/model"
	"bilibili-up-admin/internal/repository"
	appruntime "bilibili-up-admin/internal/runtime"
	"bilibili-up-admin/pkg/bilibili"
)

const DefaultTrendCacheTTL = 10 * time.Minute
const DefaultTrendStaleCacheTTL = 24 * time.Hour

const (
	TagInfoPollMaxPerRun = 5
	TagInfoPollInterval  = 1500 * time.Millisecond
)

// TrendService 热度服务
type TrendService struct {
	runtime    *appruntime.Store
	repo       *repository.TagRankingRepository
	cache      *appcache.TrendCache
	refreshMu  sync.Mutex
	refreshing map[string]struct{}
}

// NewTrendService 创建热度服务
func NewTrendService(
	runtime *appruntime.Store,
	repo *repository.TagRankingRepository,
) *TrendService {
	return &TrendService{
		runtime:    runtime,
		repo:       repo,
		cache:      appcache.NewTrendCache(DefaultTrendCacheTTL, DefaultTrendStaleCacheTTL, log.Printf),
		refreshing: make(map[string]struct{}),
	}
}

func (s *TrendService) biliClient() (*bilibili.Client, error) {
	if s.runtime == nil || s.runtime.BilibiliClient() == nil {
		return nil, fmt.Errorf("bilibili login is not configured")
	}
	return s.runtime.BilibiliClient(), nil
}

// TagRankingResult 标签排行结果
type TagRankingResult struct {
	Tags     []model.TagRanking `json:"tags"`
	Date     string             `json:"date"`
	Category string             `json:"category"`
}

// GetTrendingTags 获取热门标签
func (s *TrendService) GetTrendingTags(ctx context.Context, category string, limit int) ([]bilibili.TrendingTag, error) {
	fetchLimit := normalizeTrendFetchLimit(category, limit)
	tags, err := s.fetchTrendingTags(ctx, category, fetchLimit)
	if err != nil {
		return nil, err
	}
	s.setTrendingTagsCache(category, tags, DefaultTrendCacheTTL, "bilibili-live")
	return limitTrendingTagsForService(tags, limit), nil
}

func (s *TrendService) fetchTrendingTags(ctx context.Context, category string, limit int) ([]bilibili.TrendingTag, error) {
	client, err := s.biliClient()
	if err != nil {
		return nil, err
	}
	tags, err := client.GetTrendingTagsByCategory(ctx, category, limit)
	if err != nil {
		log.Printf("[trend.tags] fetch failed category=%q limit=%d err=%v", category, limit, err)
		return nil, err
	}
	if len(tags) == 0 {
		log.Printf("[trend.tags] fetch empty category=%q limit=%d", category, limit)
	}
	log.Printf("[trend.tags] fetched category=%q limit=%d count=%d", category, limit, len(tags))
	return tags, nil
}

// GetTrendingTagsSmart 优先读取缓存，不存在或过期时回源并刷新
func (s *TrendService) GetTrendingTagsSmart(ctx context.Context, category string, limit int, ttl time.Duration) ([]bilibili.TrendingTag, error) {
	if ttl <= 0 {
		ttl = DefaultTrendCacheTTL
	}
	fetchLimit := normalizeTrendFetchLimit(category, limit)
	if tags, status, ok := s.cache.GetTrendingTags(category, limit); ok {
		if status == appcache.CacheStale {
			s.refreshTrendingTagsAsync(category, fetchLimit, ttl)
		}
		return tags, nil
	}
	if rankings, status, ok := s.cache.GetRankings(category, limit); ok {
		if status == appcache.CacheStale {
			s.refreshTrendingTagsAsync(category, fetchLimit, ttl)
		}
		tags := toTrendingTags(rankings)
		s.cache.SetTrendingTags(category, tags, ttl, "rankings-cache")
		return tags, nil
	}
	if s.repo != nil {
		rankings, err := s.repo.GetLatestByCategory(ctx, category, fetchLimit)
		if err == nil && len(rankings) > 0 {
			log.Printf("[trend.cache] warm type=tags category=%q count=%d source=db-cold-start", category, len(rankings))
			s.setRankingsCacheForCategory(category, rankings, ttl, "db-cold-start")
			s.refreshTrendingTagsAsync(category, fetchLimit, ttl)
			return limitTrendingTagsForService(toTrendingTags(rankings), limit), nil
		}
		if err != nil {
			log.Printf("[trend.cache] db cold-start failed category=%q limit=%d err=%v", category, fetchLimit, err)
		}
	}
	tags, err := s.fetchTrendingTags(ctx, category, fetchLimit)
	if err != nil {
		return nil, err
	}
	s.setTrendingTagsCache(category, tags, ttl, "bilibili-sync")
	return limitTrendingTagsForService(tags, limit), nil
}

// GetTrendingTagsFromDB 仅返回数据库中最近一次同步的标签信息
func (s *TrendService) GetTrendingTagsFromDB(ctx context.Context, category string, limit int) ([]bilibili.TrendingTag, error) {
	if s.repo == nil {
		return nil, fmt.Errorf("tag ranking repository is not configured")
	}
	rankings, err := s.repo.GetLatestByCategory(ctx, category, limit)
	if err != nil {
		return nil, err
	}
	return toTrendingTags(rankings), nil
}

func (s *TrendService) enrichTrendingTagsWithStoredInfo(ctx context.Context, tags []bilibili.TrendingTag) []bilibili.TrendingTag {
	if s.repo == nil || len(tags) == 0 {
		return tags
	}
	enriched := make([]bilibili.TrendingTag, len(tags))
	copy(enriched, tags)
	for i := range enriched {
		row, err := s.repo.GetByTagName(ctx, enriched[i].Name)
		if err != nil || row == nil {
			continue
		}
		if enriched[i].TagID == 0 {
			enriched[i].TagID = row.TagID
		}
		if enriched[i].HotValue == 0 {
			enriched[i].HotValue = row.HotValue
		}
		enriched[i].UseCount = row.UseCount
		enriched[i].FollowCount = row.FollowCount
	}
	return enriched
}

func toTrendingTags(rankings []model.TagRanking) []bilibili.TrendingTag {
	out := make([]bilibili.TrendingTag, 0, len(rankings))
	for _, row := range rankings {
		out = append(out, bilibili.TrendingTag{
			TagID:       row.TagID,
			Name:        row.TagName,
			HotValue:    row.HotValue,
			UseCount:    row.UseCount,
			FollowCount: row.FollowCount,
			Rank:        row.Rank,
			Category:    row.Category,
		})
	}
	return out
}

func toTagRankings(tags []bilibili.TrendingTag, now time.Time) []model.TagRanking {
	type catGroup struct{ tags []bilibili.TrendingTag }
	groups := make(map[string]*catGroup)
	catOrder := make([]string, 0)
	for _, tag := range tags {
		if _, ok := groups[tag.Category]; !ok {
			groups[tag.Category] = &catGroup{}
			catOrder = append(catOrder, tag.Category)
		}
		groups[tag.Category].tags = append(groups[tag.Category].tags, tag)
	}
	for _, g := range groups {
		sort.SliceStable(g.tags, func(i, j int) bool {
			return g.tags[i].HotValue > g.tags[j].HotValue
		})
	}

	rankings := make([]model.TagRanking, 0, len(tags))
	for _, cat := range catOrder {
		for i, tag := range groups[cat].tags {
			rankings = append(rankings, model.TagRanking{
				TagName:     tag.Name,
				TagID:       tag.TagID,
				HotValue:    tag.HotValue,
				UseCount:    tag.UseCount,
				FollowCount: tag.FollowCount,
				Rank:        i + 1,
				Category:    cat,
				RecordDate:  now,
			})
		}
	}
	return rankings
}

func normalizeTrendFetchLimit(category string, limit int) int {
	minLimit := 50
	if category != "" {
		minLimit = 30
	}
	if limit <= 0 || limit < minLimit {
		return minLimit
	}
	return limit
}

func limitTrendingTagsForService(tags []bilibili.TrendingTag, limit int) []bilibili.TrendingTag {
	if limit > 0 && limit < len(tags) {
		tags = tags[:limit]
	}
	out := make([]bilibili.TrendingTag, len(tags))
	copy(out, tags)
	return out
}

func limitTagRankingsForService(rankings []model.TagRanking, limit int) []model.TagRanking {
	if limit > 0 && limit < len(rankings) {
		rankings = rankings[:limit]
	}
	out := make([]model.TagRanking, len(rankings))
	copy(out, rankings)
	return out
}

func (s *TrendService) setTrendingTagsCache(category string, tags []bilibili.TrendingTag, ttl time.Duration, source string) {
	if s.cache == nil || len(tags) == 0 {
		return
	}
	s.cache.SetTrendingTags(category, tags, ttl, source)
	rankings := toTagRankings(tags, time.Now())
	s.setRankingsCacheForCategory(category, rankings, ttl, source)
	if category == "" {
		grouped := make(map[string][]bilibili.TrendingTag)
		for _, tag := range tags {
			if tag.Category == "" {
				continue
			}
			grouped[tag.Category] = append(grouped[tag.Category], tag)
		}
		for cat, items := range grouped {
			s.cache.SetTrendingTags(cat, items, ttl, source)
		}
	}
}

func (s *TrendService) setRankingsCacheForCategory(category string, rankings []model.TagRanking, ttl time.Duration, source string) {
	if s.cache == nil || len(rankings) == 0 {
		return
	}
	if category == "" {
		s.setRankingsCache(rankings, ttl, source)
		return
	}
	s.cache.SetRankings(category, rankings, ttl, source)
	s.cache.SetTrendingTags(category, toTrendingTags(rankings), ttl, source)
}

func (s *TrendService) setRankingsCache(rankings []model.TagRanking, ttl time.Duration, source string) {
	if s.cache == nil || len(rankings) == 0 {
		return
	}
	s.cache.SetRankings("", rankings, ttl, source)
	s.cache.SetTrendingTags("", toTrendingTags(rankings), ttl, source)
	grouped := make(map[string][]model.TagRanking)
	for _, row := range rankings {
		if row.Category == "" {
			continue
		}
		grouped[row.Category] = append(grouped[row.Category], row)
	}
	for category, items := range grouped {
		s.cache.SetRankings(category, items, ttl, source)
		s.cache.SetTrendingTags(category, toTrendingTags(items), ttl, source)
	}
}

func (s *TrendService) refreshTrendingTagsAsync(category string, limit int, ttl time.Duration) {
	key := fmt.Sprintf("%s:%d", category, limit)
	if !s.beginRefresh(key) {
		log.Printf("[trend.cache] refresh skipped category=%q limit=%d reason=already_running", category, limit)
		return
	}
	log.Printf("[trend.cache] refresh scheduled category=%q limit=%d", category, limit)
	go func() {
		defer s.endRefresh(key)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		tags, err := s.fetchTrendingTags(ctx, category, limit)
		if err != nil {
			log.Printf("[trend.cache] refresh failed category=%q limit=%d err=%v", category, limit, err)
			return
		}
		s.setTrendingTagsCache(category, tags, ttl, "bilibili-refresh")
		log.Printf("[trend.cache] refresh done category=%q limit=%d count=%d", category, limit, len(tags))
	}()
}

func (s *TrendService) beginRefresh(key string) bool {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	if _, ok := s.refreshing[key]; ok {
		return false
	}
	s.refreshing[key] = struct{}{}
	return true
}

func (s *TrendService) endRefresh(key string) {
	s.refreshMu.Lock()
	delete(s.refreshing, key)
	s.refreshMu.Unlock()
}

// EnsureLatestTags 确保缓存可用，必要时刷新；返回缓存内容与是否发生刷新
func (s *TrendService) EnsureLatestTags(ctx context.Context, category string, limit int, ttl time.Duration) ([]model.TagRanking, bool, error) {
	if limit <= 0 {
		limit = 50
	}
	if ttl <= 0 {
		ttl = DefaultTrendCacheTTL
	}

	if rankings, status, ok := s.cache.GetRankings(category, limit); ok {
		if status == appcache.CacheStale {
			s.refreshTrendingTagsAsync(category, normalizeTrendFetchLimit(category, limit), ttl)
		}
		return rankings, false, nil
	}
	if tags, status, ok := s.cache.GetTrendingTags(category, limit); ok {
		if status == appcache.CacheStale {
			s.refreshTrendingTagsAsync(category, normalizeTrendFetchLimit(category, limit), ttl)
		}
		rankings := toTagRankings(tags, time.Now())
		s.cache.SetRankings(category, rankings, ttl, "tags-cache")
		return rankings, false, nil
	}

	if s.repo != nil {
		rankings, err := s.repo.GetLatestByCategory(ctx, category, normalizeTrendFetchLimit(category, limit))
		if err == nil && len(rankings) > 0 {
			log.Printf("[trend.cache] warm type=rankings category=%q count=%d source=db-cold-start", category, len(rankings))
			s.setRankingsCacheForCategory(category, rankings, ttl, "db-cold-start")
			s.refreshTrendingTagsAsync(category, normalizeTrendFetchLimit(category, limit), ttl)
			return limitTagRankingsForService(rankings, limit), false, nil
		}
		if err != nil {
			log.Printf("[trend.cache] db rankings cold-start failed category=%q limit=%d err=%v", category, limit, err)
		}
	}

	tags, fetchErr := s.fetchTrendingTags(ctx, category, normalizeTrendFetchLimit(category, limit))
	if fetchErr != nil {
		return nil, false, fetchErr
	}
	s.setTrendingTagsCache(category, tags, ttl, "bilibili-ensure")
	if saveErr := s.SaveTagRankings(ctx, tags); saveErr != nil {
		log.Printf("[trend.cache] persist rankings failed category=%q count=%d err=%v", category, len(tags), saveErr)
	}
	return limitTagRankingsForService(toTagRankings(tags, time.Now()), limit), true, nil
}

// GetTagDetail 获取标签详情
func (s *TrendService) GetTagDetail(ctx context.Context, tagName string, page, pageSize int) (*bilibili.TagRanking, error) {
	client, err := s.biliClient()
	if err != nil {
		return nil, err
	}
	return client.GetTagRanking(ctx, tagName, page, pageSize)
}

// GetVideoRanking 获取视频排行
func (s *TrendService) GetVideoRanking(ctx context.Context, category string, limit int) (*bilibili.VideoRanking, error) {
	client, err := s.biliClient()
	if err != nil {
		return nil, err
	}
	return client.GetCategoryRanking(ctx, category, limit)
}

// SaveTagRankings 保存标签排行（各分区内按热度降序分配 rank，保证排名稳定）
func (s *TrendService) SaveTagRankings(ctx context.Context, tags []bilibili.TrendingTag) error {
	rankings := toTagRankings(tags, time.Now())
	s.setRankingsCache(rankings, DefaultTrendCacheTTL, "save")
	if s.repo == nil || len(rankings) == 0 {
		log.Printf("[trend.cache] persist skipped count=%d reason=repo_unavailable_or_empty", len(rankings))
		return nil
	}
	return s.repo.BatchCreate(ctx, rankings)
}

// SyncTagInfoHotValues 基于缓存标签列表，高频轮询 tag info 更新热度
func (s *TrendService) SyncTagInfoHotValues(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 50
	}

	cached, _, _ := s.cache.GetRankings("", limit)
	if len(cached) == 0 {
		if tags, _, ok := s.cache.GetTrendingTags("", limit); ok {
			cached = toTagRankings(tags, time.Now())
		}
	}
	if len(cached) == 0 && s.repo != nil {
		rows, err := s.repo.GetLatest(ctx, limit)
		if err == nil && len(rows) > 0 {
			log.Printf("[trend.cache] warm type=taginfo category=%q count=%d source=db-cold-start", "", len(rows))
			s.setRankingsCache(rows, DefaultTrendCacheTTL, "db-cold-start")
			cached = rows
		}
		if err != nil {
			log.Printf("[trend.cache] db taginfo cold-start failed limit=%d err=%v", limit, err)
		}
	}
	if len(cached) == 0 {
		rankings, _, ensureErr := s.EnsureLatestTags(ctx, "", limit, DefaultTrendCacheTTL)
		if ensureErr != nil {
			return 0, ensureErr
		}
		cached = rankings
	}
	if len(cached) == 0 {
		log.Printf("[trend.cache] taginfo skipped reason=empty_cache limit=%d", limit)
		return 0, nil
	}

	client, err := s.biliClient()
	if err != nil {
		return 0, err
	}

	ordered := make([]model.TagRanking, 0, len(cached))
	seen := make(map[string]struct{})
	for _, row := range cached {
		if row.TagName == "" {
			continue
		}
		if _, ok := seen[row.TagName]; ok {
			continue
		}
		seen[row.TagName] = struct{}{}
		ordered = append(ordered, row)
	}

	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Category == ordered[j].Category {
			return ordered[i].Rank < ordered[j].Rank
		}
		return ordered[i].Category < ordered[j].Category
	})

	if len(ordered) > limit {
		ordered = ordered[:limit]
	}
	refreshIndexes := buildGradualRefreshIndexes(len(ordered), TagInfoPollMaxPerRun)
	refreshSet := make(map[int]struct{}, len(refreshIndexes))
	for _, index := range refreshIndexes {
		refreshSet[index] = struct{}{}
	}

	tags := make([]bilibili.TrendingTag, 0, len(ordered))
	refreshed := int(0)
	for i, row := range ordered {
		tag := bilibili.TrendingTag{
			TagID:       row.TagID,
			Name:        row.TagName,
			HotValue:    row.HotValue,
			Rank:        row.Rank,
			Category:    row.Category,
			UseCount:    row.UseCount,
			FollowCount: row.FollowCount,
		}

		if _, ok := refreshSet[i]; ok {
			if refreshed > 0 {
				if ctx == nil {
					time.Sleep(TagInfoPollInterval)
				} else {
					timer := time.NewTimer(TagInfoPollInterval)
					select {
					case <-ctx.Done():
						timer.Stop()
						return refreshed, ctx.Err()
					case <-timer.C:
					}
				}
			}

			info, infoErr := client.GetTagInfo(ctx, row.TagName)
			if infoErr == nil {
				tag.TagID = info.TagID
				tag.HotValue = info.HotValue
				tag.UseCount = info.UseCount
				tag.FollowCount = info.FollowCount
				refreshed++
			}
		}

		tags = append(tags, tag)
	}

	if len(tags) == 0 {
		return 0, fmt.Errorf("sync tag info failed: empty cache")
	}

	s.setTrendingTagsCache("", tags, DefaultTrendCacheTTL, "taginfo-poll")
	if refreshed == 0 {
		log.Printf("[trend.cache] taginfo no_remote_refresh limit=%d cache_count=%d", limit, len(tags))
		return 0, nil
	}
	if err := s.SaveTagRankings(ctx, tags); err != nil {
		return 0, err
	}
	log.Printf("[trend.cache] taginfo refreshed=%d cache_count=%d", refreshed, len(tags))
	return refreshed, nil
}

func buildGradualRefreshIndexes(total, maxPerRun int) []int {
	if total <= 0 || maxPerRun <= 0 {
		return nil
	}
	if maxPerRun > total {
		maxPerRun = total
	}

	window := time.Now().Unix() / 60
	start := int(window % int64(total))
	indexes := make([]int, 0, maxPerRun)
	for i := 0; i < maxPerRun; i++ {
		indexes = append(indexes, (start+i)%total)
	}
	return indexes
}

// GetHistoricalRankings 获取历史排行
func (s *TrendService) GetHistoricalRankings(ctx context.Context, date string, limit int) ([]model.TagRanking, error) {
	recordDate, err := time.Parse("2006-01-02", date)
	if err != nil {
		return nil, fmt.Errorf("invalid date format: %w", err)
	}

	return s.repo.ListByDate(ctx, recordDate, limit)
}

// GetLatestRankings 获取最新排行
func (s *TrendService) GetLatestRankings(ctx context.Context, category string, limit int) ([]model.TagRanking, error) {
	if limit <= 0 {
		limit = 50
	}
	if rankings, status, ok := s.cache.GetRankings(category, limit); ok {
		if status == appcache.CacheStale {
			s.refreshTrendingTagsAsync(category, normalizeTrendFetchLimit(category, limit), DefaultTrendCacheTTL)
		}
		return rankings, nil
	}
	if tags, status, ok := s.cache.GetTrendingTags(category, limit); ok {
		if status == appcache.CacheStale {
			s.refreshTrendingTagsAsync(category, normalizeTrendFetchLimit(category, limit), DefaultTrendCacheTTL)
		}
		rankings := toTagRankings(tags, time.Now())
		s.cache.SetRankings(category, rankings, DefaultTrendCacheTTL, "tags-cache")
		return limitTagRankingsForService(rankings, limit), nil
	}
	if s.repo != nil {
		var (
			rankings []model.TagRanking
			err      error
		)
		if category != "" {
			rankings, err = s.repo.GetLatestByCategory(ctx, category, normalizeTrendFetchLimit(category, limit))
		} else {
			rankings, err = s.repo.GetLatest(ctx, normalizeTrendFetchLimit(category, limit))
		}
		if err == nil && len(rankings) > 0 {
			log.Printf("[trend.cache] warm type=latest category=%q count=%d source=db-cold-start", category, len(rankings))
			s.setRankingsCacheForCategory(category, rankings, DefaultTrendCacheTTL, "db-cold-start")
			s.refreshTrendingTagsAsync(category, normalizeTrendFetchLimit(category, limit), DefaultTrendCacheTTL)
			return limitTagRankingsForService(rankings, limit), nil
		}
		if err != nil {
			log.Printf("[trend.cache] db latest cold-start failed category=%q limit=%d err=%v", category, limit, err)
		}
	}
	tags, err := s.GetTrendingTagsSmart(ctx, category, limit, DefaultTrendCacheTTL)
	if err != nil {
		return nil, err
	}
	return limitTagRankingsForService(toTagRankings(tags, time.Now()), limit), nil
}

// TrendStats 热度统计
type TrendStats struct {
	TotalTags    int64  `json:"total_tags"`
	TotalRecords int64  `json:"total_records"`
	LatestDate   string `json:"latest_date"`
}

// GetStats 获取热度统计
func (s *TrendService) GetStats(ctx context.Context) (*TrendStats, error) {
	var totalTags int64
	var totalRecords int64
	var latestDate time.Time

	// 这里简化处理，实际可以使用SQL查询
	rankings, _ := s.repo.GetLatest(ctx, 1)
	if len(rankings) > 0 {
		latestDate = rankings[0].RecordDate
	}

	return &TrendStats{
		TotalTags:    totalTags,
		TotalRecords: totalRecords,
		LatestDate:   latestDate.Format("2006-01-02"),
	}, nil
}

// SearchTag 搜索标签
func (s *TrendService) SearchTag(ctx context.Context, keyword string, page, pageSize int) ([]bilibili.TagRanking, error) {
	client, err := s.biliClient()
	if err != nil {
		return nil, err
	}
	return client.SearchTag(ctx, keyword, page, pageSize)
}

// VideoInfo 视频信息
type VideoInfo struct {
	BVID     string   `json:"bvid"`
	Title    string   `json:"title"`
	Owner    string   `json:"owner"`
	OwnerID  int64    `json:"owner_id"`
	Duration int      `json:"duration"`
	View     int      `json:"view"`
	Like     int      `json:"like"`
	Coin     int      `json:"coin"`
	Tags     []string `json:"tags"`
	Pic      string   `json:"pic"`
}

// GetVideoInfo 获取视频信息
func (s *TrendService) GetVideoInfo(ctx context.Context, bvID string) (*bilibili.VideoInfo, error) {
	client, err := s.biliClient()
	if err != nil {
		return nil, err
	}
	return client.GetVideoInfo(ctx, bvID)
}

// DailySync 每日同步热度数据
func (s *TrendService) DailySync(ctx context.Context) error {
	_, err := s.SyncTagInfoHotValues(ctx, 50)
	if err == nil {
		return nil
	}

	_, _, err = s.EnsureLatestTags(ctx, "", 50, DefaultTrendCacheTTL)
	if err != nil {
		return fmt.Errorf("daily sync trending tags failed: %w", err)
	}
	return nil
}
