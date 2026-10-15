// Package spectest has the helpers of a product's WebSocket spec conformance test: the spec's
// example payloads, strict decoding, and comparing what the SDK sends with what the spec shows.
package spectest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/internal/ws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

const settle = 2 * time.Second

// Example is one `examples:` entry of one message in the spec, as JSON.
type Example struct {
	Name    string
	Payload []byte
}

// LoadExamples reads every example of every message in the spec at path, by message name.
func LoadExamples(t *testing.T, path string) map[string][]Example {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	var doc struct {
		Components struct {
			Messages map[string]struct {
				Examples []struct {
					Name    string `yaml:"name"`
					Payload any    `yaml:"payload"`
				} `yaml:"examples"`
			} `yaml:"messages"`
		} `yaml:"components"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &doc))

	examples := map[string][]Example{}
	for message, m := range doc.Components.Messages {
		for _, ex := range m.Examples {
			payload, err := json.Marshal(ex.Payload)
			require.NoError(t, err, "%s/%s", message, ex.Name)
			examples[message] = append(examples[message], Example{Name: ex.Name, Payload: payload})
		}
	}
	return examples
}

// Strict decodes data into v and fails on any key v has no field for.
func Strict(t *testing.T, data []byte, v any) {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	require.NoError(t, dec.Decode(v), "%T does not model everything in:\n%s", v, data)
}

// FrameOf decodes an example as the connection would, strictly at the envelope.
func FrameOf(t *testing.T, ex Example) ws.Frame {
	t.Helper()
	var frame ws.Frame
	Strict(t, ex.Payload, &frame)
	frame.Raw = ex.Payload
	return frame
}

// EventAs turns a frame into the product's event and requires it to be a T.
func EventAs[T, E any](t *testing.T, toEvent func(ws.Frame) E, frame ws.Frame) T {
	t.Helper()
	event := toEvent(frame)
	typed, ok := any(event).(T)
	require.True(t, ok, "%s became %T, not %T", frame.ResultType, event, *new(T))
	return typed
}

// Decode reads JSON into a map, so an assertion sees exactly the keys on the wire.
func Decode(t *testing.T, content json.RawMessage) map[string]any {
	t.Helper()
	var got map[string]any
	require.NoError(t, json.Unmarshal(content, &got))
	return got
}

// Receive waits for one value from ch, and fails the test if none arrives.
func Receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(settle):
		t.Fatalf("no %T within %v", *new(T), settle)
		return *new(T)
	}
}

// Raw encodes v back to JSON.
func Raw(t *testing.T, v any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	return data
}

// Keys lists a map's keys.
func Keys(m map[string]any) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// Num and Str read what json.Unmarshal put in a map[string]any; a missing key reads as zero.
func Num(v any) float64 {
	f, _ := v.(float64)
	return f
}

// Str is [Num] for strings.
func Str(v any) string {
	s, _ := v.(string)
	return s
}

// WithClientRequestID is the example's frame answering the command the SDK actually sent,
// for the commands that mint their own id.
func WithClientRequestID(t *testing.T, ex Example, crid string) map[string]any {
	t.Helper()
	frame := Decode(t, ex.Payload)
	frame["data"].(map[string]any)["clientRequestId"] = crid
	return frame
}

// AssertSends checks a command against the spec's: every documented key with its documented
// value, and nothing undocumented unless it is a zero value the encoder could not leave out.
func AssertSends(t *testing.T, sent, documented map[string]any) {
	t.Helper()
	for key, want := range documented {
		assert.Equal(t, want, sent[key], "content.%s", key)
	}
	for key, value := range sent {
		if _, ok := documented[key]; !ok {
			assert.Zero(t, value, "content.%s is sent but not in the spec's example", key)
		}
	}
}

// DocumentedContent is every content key the spec shows for a command, across its examples.
func DocumentedContent(t *testing.T, examples []Example) map[string]any {
	t.Helper()
	union := map[string]any{}
	for _, ex := range examples {
		for key, value := range Decode(t, Raw(t, Decode(t, ex.Payload)["content"])) {
			union[key] = value
		}
	}
	return union
}

// ContentProperties is every content key the spec's schema declares for a command, whether or
// not an example shows it, by message name.
func ContentProperties(t *testing.T, path string) map[string]map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	var doc struct {
		Components struct {
			Messages map[string]struct {
				Payload struct {
					Properties struct {
						Content struct {
							Properties map[string]any `yaml:"properties"`
						} `yaml:"content"`
					} `yaml:"properties"`
				} `yaml:"payload"`
			} `yaml:"messages"`
		} `yaml:"components"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &doc))

	properties := map[string]map[string]any{}
	for message, m := range doc.Components.Messages {
		properties[message] = m.Payload.Properties.Content.Properties
	}
	return properties
}

// AssertTagsDocumented checks that content, a command's content struct, sends no key the spec
// does not know.
func AssertTagsDocumented(t *testing.T, content any, documented map[string]any) {
	t.Helper()
	typ := reflect.TypeOf(content)
	for i := range typ.NumField() {
		name, _, _ := strings.Cut(typ.Field(i).Tag.Get("json"), ",")
		assert.Contains(t, documented, name, "%s sends %q, which the spec does not mention", typ.Name(), name)
	}
}

// AssertCarried checks that every value in documented, as [Decode] reads it, reaches public in
// the field of the same name, ignoring case, through nested objects and arrays. Keys in dropped
// are ones the SDK does not pass on.
func AssertCarried(t *testing.T, documented any, public any, dropped ...string) {
	t.Helper()
	assertCarried(t, documented, reflect.ValueOf(public), "", dropped)
}

func assertCarried(t *testing.T, documented any, public reflect.Value, path string, dropped []string) {
	t.Helper()
	for public.Kind() == reflect.Pointer || public.Kind() == reflect.Interface {
		public = public.Elem()
	}
	switch doc := documented.(type) {
	case map[string]any:
		if !assert.Equal(t, reflect.Struct, public.Kind(), "%s: an object carried as %s", path, public.Kind()) {
			return
		}
		for key, value := range doc {
			field := public.FieldByNameFunc(func(name string) bool { return strings.EqualFold(name, key) })
			if !field.IsValid() {
				assert.Contains(t, dropped, key, "%s.%s is not carried to %s", path, key, public.Type())
				continue
			}
			assertCarried(t, value, field, path+"."+key, dropped)
		}
	case []any:
		if !assert.Equal(t, reflect.Slice, public.Kind(), "%s: an array carried as %s", path, public.Kind()) ||
			!assert.Equal(t, len(doc), public.Len(), "%s: length", path) {
			return
		}
		for i, item := range doc {
			assertCarried(t, item, public.Index(i), fmt.Sprintf("%s[%d]", path, i), dropped)
		}
	case float64:
		assert.Equal(t, fmt.Sprint(int64(doc)), fmt.Sprint(public.Interface()), path)
	default:
		assert.Equal(t, fmt.Sprint(doc), fmt.Sprint(public.Interface()), path)
	}
}
