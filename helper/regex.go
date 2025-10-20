package helper

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/hdt3213/rdb/model"
)

type decoder interface {
	Parse(cb func(object model.RedisObject) bool) error
}

type regexDecoder struct {
	reg *regexp.Regexp
	dec decoder
}

func (d *regexDecoder) Parse(cb func(object model.RedisObject) bool) error {
	return d.dec.Parse(func(object model.RedisObject) bool {
		if d.reg.MatchString(object.GetKey()) {
			return cb(object)
		}
		return true
	})
}

// regexWrapper returns
func regexWrapper(d decoder, expr string) (*regexDecoder, error) {
	reg, err := regexp.Compile(expr)
	if err != nil {
		return nil, fmt.Errorf("illegal regex expression: %v", expr)
	}
	return &regexDecoder{
		dec: d,
		reg: reg,
	}, nil
}

// RegexOption enable regex filters
type RegexOption *string

// WithRegexOption creates a WithRegexOption from regex expression
func WithRegexOption(expr string) RegexOption {
	return &expr
}

// noExpiredDecoder filter all expired keys
type noExpiredDecoder struct {
	dec decoder
}

func (d *noExpiredDecoder) Parse(cb func(object model.RedisObject) bool) error {
	now := time.Now()
	return d.dec.Parse(func(object model.RedisObject) bool {
		expiration := object.GetExpiration()
		if expiration == nil || expiration.After(now) {
			return cb(object)
		}
		return true
	})
}

// NoExpiredOption tells decoder to filter all expired keys
type NoExpiredOption bool

// WithNoExpiredOption tells decoder to filter all expired keys
func WithNoExpiredOption() NoExpiredOption {
	return NoExpiredOption(true)
}

type keySizeFilterDecoder struct {
	dec decoder
}

func (d *keySizeFilterDecoder) Parse(cb func(object model.RedisObject) bool) error {
	return d.dec.Parse(func(object model.RedisObject) bool {
		if matchBigKey(object) {
			return cb(object)
		}
		return true
	})
}

func matchBigKey(obj model.RedisObject) bool {
	//禁止使用大Key。要求String大小≤10kb，Hash字段数≤1000或大小≤100kb，List元素数量≤1000，Set成员数量≤1000，ZSet成员数量≤1000。
	switch obj.GetType() {
	case model.StringType:
		if obj.GetSize() > 10*1024 {
			return true
		}
	case model.ListType:
		if obj.GetElemCount() > 1000 {
			return true
		}
	case model.HashType:
		if obj.GetElemCount() > 1000 || obj.GetSize() > 100*1024 {
			return true
		}
	case model.SetType:
		if obj.GetElemCount() > 1000 {
			return true
		}
	case model.ZSetType:
		if obj.GetElemCount() > 1000 {
			return true
		}
	}
	return false
}

type KeySizeFilterOption bool

func WithKeySizeFilterOption() KeySizeFilterOption {
	return KeySizeFilterOption(true)
}

type ExpirationOption string

func WithExpirationOption(expr string) ExpirationOption {
	return ExpirationOption(expr)
}

// expirationDecoder returns entries with expiration times and expiration within the range.
type expirationDecoder struct {
	dec             decoder
	expirationRange []int64
}

// Parse returns entries with expiration times and expiration within the range.

func (d *expirationDecoder) Parse(cb func(object model.RedisObject) bool) error {
	return d.dec.Parse(func(object model.RedisObject) bool {
		expiration := object.GetExpiration()
		if expiration != nil {
			timestamp := expiration.Unix()
			if timestamp >= d.expirationRange[0] && timestamp <= d.expirationRange[1] {
				return cb(object)
			}
		}
		return true
	})
}

// noExpirationDecoder returns entries without expiration
type noExpirationDecoder struct {
	dec decoder
}

func (d *noExpirationDecoder) Parse(cb func(object model.RedisObject) bool) error {
	return d.dec.Parse(func(object model.RedisObject) bool {
		expiration := object.GetExpiration()
		if expiration == nil {
			return cb(object)
		}
		return true
	})
}

func parseExpireExpr(s string) ([]int64, error) {
	parseValue := func(s string) (int64, error) {
		if s == "now" {
			return time.Now().Unix(), nil
		}
		if s == "inf" {
			return math.MaxInt64, nil
		}
		return strconv.ParseInt(s, 10, 64)
	}

	parts := strings.Split(s, "~")
	if len(parts) != 2 {
		return nil, errors.New("illegal expr, should be timestamp1~timestamp2")
	}

	min, err := parseValue(parts[0])
	if err != nil {
		return nil, fmt.Errorf("illegal range begin")
	}
	max, err := parseValue(parts[1])
	if err != nil {
		return nil, fmt.Errorf("illegal range end")
	}
	return []int64{min, max}, nil
}

func wrapDecoder(dec decoder, options ...interface{}) (decoder, error) {
	var regexOpt RegexOption
	var noExpiredOpt NoExpiredOption
	var expirationOpt ExpirationOption
	var keySizeFilterOpt KeySizeFilterOption
	for _, opt := range options {
		switch o := opt.(type) {
		case RegexOption:
			regexOpt = o
		case NoExpiredOption:
			noExpiredOpt = o
		case ExpirationOption:
			expirationOpt = o
		case KeySizeFilterOption:
			keySizeFilterOpt = o
		}
	}
	if regexOpt != nil {
		var err error
		dec, err = regexWrapper(dec, *regexOpt)
		if err != nil {
			return nil, err
		}
	}
	if noExpiredOpt {
		dec = &noExpiredDecoder{
			dec: dec,
		}
	}
	if keySizeFilterOpt {
		dec = &keySizeFilterDecoder{
			dec: dec,
		}
	}
	if expirationOpt != "" {
		if expirationOpt == "noexpire" {
			dec = &noExpirationDecoder{
				dec: dec,
			}
		} else if expirationOpt == "anyexpire" {
			dec = &expirationDecoder{
				dec:             dec,
				expirationRange: []int64{0, math.MaxInt64},
			}
		} else {
			rng, err := parseExpireExpr(string(expirationOpt))
			if err != nil {
				return nil, err
			}
			dec = &expirationDecoder{
				dec:             dec,
				expirationRange: rng,
			}
		}
	}
	return dec, nil
}
