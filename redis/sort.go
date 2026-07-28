package redis

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
)

// Sort mirrors go-redis Sort options for the supported subset.
// Supported: Alpha, Order (ASC/DESC), Offset, Count, By ("nosort" only).
// Unsupported: Get patterns.
type Sort struct {
	By     string
	Offset int64
	Count  int64
	Get    []string
	Order  string
	Alpha  bool
}

// sortConfig is the normalized form of Sort. Keeping "is there a limit" as its
// own field is what makes &Sort{Alpha: true} return every element, matching
// go-redis, which only emits LIMIT when Offset or Count is set.
type sortConfig struct {
	order    string
	alpha    bool
	nosort   bool
	offset   int64
	count    int64
	hasLimit bool
}

// Sort orders the elements of a list or set stored at key.
func (c *Client) Sort(ctx context.Context, key string, opt *Sort) *StringSliceCmd {
	cmd := &StringSliceCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}

	cfg, err := normalizeSortOptions(opt)
	if err != nil {
		cmd.err = err
		return cmd
	}

	values, err := c.sortSource(ctx, key)
	if err != nil {
		cmd.err = err
		return cmd
	}

	out, err := sortAndSliceValues(values, cfg)
	if err != nil {
		cmd.err = err
		return cmd
	}
	cmd.val = out
	return cmd
}

// sortSource resolves the elements to sort from the key's recorded type, so a
// list and a set are never confused for one another.
func (c *Client) sortSource(ctx context.Context, key string) ([]string, error) {
	m, found, err := c.readMeta(ctx, key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	switch m.typ {
	case typeList:
		return c.listValues(ctx, key)
	case typeSet:
		return c.setValues(ctx, key)
	default:
		return nil, ErrWrongType
	}
}

func normalizeSortOptions(opt *Sort) (sortConfig, error) {
	cfg := sortConfig{order: "ASC"}
	if opt == nil {
		return cfg, nil
	}

	cfg.alpha = opt.Alpha
	cfg.offset = opt.Offset
	cfg.count = opt.Count
	cfg.hasLimit = opt.Offset != 0 || opt.Count != 0

	cfg.order = strings.ToUpper(strings.TrimSpace(opt.Order))
	if cfg.order == "" {
		cfg.order = "ASC"
	}
	if cfg.order != "ASC" && cfg.order != "DESC" {
		return sortConfig{}, errors.New("rediscompat: Sort order must be ASC or DESC")
	}

	by := strings.TrimSpace(opt.By)
	if by != "" {
		if !strings.EqualFold(by, "nosort") {
			return sortConfig{}, errors.New("rediscompat: Sort currently supports BY only as \"nosort\"")
		}
		cfg.nosort = true
	}
	if len(opt.Get) > 0 {
		return sortConfig{}, errors.New("rediscompat: Sort GET patterns are not supported yet")
	}
	if cfg.offset < 0 {
		cfg.offset = 0
	}
	return cfg, nil
}

func sortAndSliceValues(values []string, cfg sortConfig) ([]string, error) {
	out := append([]string(nil), values...)

	if !cfg.nosort {
		if cfg.alpha {
			sort.Strings(out)
		} else {
			// Parse first so a non numeric element is reported as an error
			// instead of quietly producing an arbitrary order.
			numeric := make([]float64, len(out))
			for i := range out {
				parsed, err := strconv.ParseFloat(out[i], 64)
				if err != nil {
					return nil, ErrScoreNotDouble
				}
				numeric[i] = parsed
			}
			indexes := make([]int, len(out))
			for i := range indexes {
				indexes[i] = i
			}
			sort.SliceStable(indexes, func(a, b int) bool {
				return numeric[indexes[a]] < numeric[indexes[b]]
			})
			sorted := make([]string, len(out))
			for i, idx := range indexes {
				sorted[i] = out[idx]
			}
			out = sorted
		}
	}

	if cfg.order == "DESC" {
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
	}

	if !cfg.hasLimit {
		return out, nil
	}

	start := int(min(cfg.offset, int64(len(out))))
	end := len(out)
	if cfg.count > 0 {
		if remaining := int64(len(out) - start); cfg.count < remaining {
			end = start + int(cfg.count)
		}
	} else if cfg.count == 0 {
		end = start
	}
	return out[start:end], nil
}
