package redis

import (
	"strconv"
	"time"
)

type baseCmd struct {
	err error
}

func (cmd *baseCmd) Err() error {
	return cmd.err
}

type StatusCmd struct {
	baseCmd
	val string
}

func (cmd *StatusCmd) Val() string {
	return cmd.val
}

func (cmd *StatusCmd) Result() (string, error) {
	return cmd.val, cmd.err
}

type StringCmd struct {
	baseCmd
	val string
}

func (cmd *StringCmd) Val() string {
	return cmd.val
}

func (cmd *StringCmd) Result() (string, error) {
	return cmd.val, cmd.err
}

func (cmd *StringCmd) Bytes() ([]byte, error) {
	if cmd.err != nil {
		return nil, cmd.err
	}
	return []byte(cmd.val), nil
}

func (cmd *StringCmd) Int() (int, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return strconv.Atoi(cmd.val)
}

func (cmd *StringCmd) Int64() (int64, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return strconv.ParseInt(cmd.val, 10, 64)
}

type IntCmd struct {
	baseCmd
	val int64
}

func (cmd *IntCmd) Val() int64 {
	return cmd.val
}

func (cmd *IntCmd) Result() (int64, error) {
	return cmd.val, cmd.err
}

type SliceCmd struct {
	baseCmd
	val []interface{}
}

func (cmd *SliceCmd) Val() []interface{} {
	return cmd.val
}

func (cmd *SliceCmd) Result() ([]interface{}, error) {
	return cmd.val, cmd.err
}

type BoolCmd struct {
	baseCmd
	val bool
}

func (cmd *BoolCmd) Val() bool {
	return cmd.val
}

func (cmd *BoolCmd) Result() (bool, error) {
	return cmd.val, cmd.err
}

type DurationCmd struct {
	baseCmd
	val time.Duration
}

func (cmd *DurationCmd) Val() time.Duration {
	return cmd.val
}

func (cmd *DurationCmd) Result() (time.Duration, error) {
	return cmd.val, cmd.err
}

type MapStringStringCmd struct {
	baseCmd
	val map[string]string
}

func (cmd *MapStringStringCmd) Val() map[string]string {
	return cmd.val
}

func (cmd *MapStringStringCmd) Result() (map[string]string, error) {
	return cmd.val, cmd.err
}

type StringSliceCmd struct {
	baseCmd
	val []string
}

func (cmd *StringSliceCmd) Val() []string {
	return cmd.val
}

func (cmd *StringSliceCmd) Result() ([]string, error) {
	return cmd.val, cmd.err
}

type ScanCmd struct {
	baseCmd
	val    []string
	cursor uint64
}

func (cmd *ScanCmd) Val() []string {
	return cmd.val
}

func (cmd *ScanCmd) Cursor() uint64 {
	return cmd.cursor
}

func (cmd *ScanCmd) Result() ([]string, uint64, error) {
	return cmd.val, cmd.cursor, cmd.err
}
