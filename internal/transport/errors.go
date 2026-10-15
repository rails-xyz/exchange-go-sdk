package transport

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rails-xyz/exchange-go-sdk/internal/headers"
	"github.com/rails-xyz/exchange-go-sdk/rails"
)

// envelope covers every shape a non-2xx carries: {"error":{slug,code},"message"},
// {"body","statusCode"} from the account reads, and a bare {"message"}.
type envelope struct {
	Message string `json:"message"`
	Body    string `json:"body"`
	Error   *struct {
		Slug string `json:"slug"`
		Code string `json:"code"`
	} `json:"error"`
}

// maxErrorBody bounds what an unparseable error body contributes to a message.
const maxErrorBody = 256

// ParseError turns a non-2xx response into a [rails.Error]. Nothing is assumed about the body:
// several routes return no JSON at all on an internal failure.
func ParseError(op string, resp *http.Response, body []byte) *rails.Error {
	err := &rails.Error{
		Op:            op,
		StatusCode:    resp.StatusCode,
		RetryAfter:    headers.RetryAfter(resp.Header, time.Now()),
		APIQuotaUsed:  headerInt(resp.Header, "X-Api-Quota-Used"),
		APIQuotaLimit: headerInt(resp.Header, "X-Api-Quota-Limit"),
	}

	var env envelope
	if json.Unmarshal(body, &env) == nil {
		switch {
		case env.Error != nil:
			err.Slug, err.Code, err.Message = env.Error.Slug, rails.Code(env.Error.Code), env.Message
		case env.Body != "":
			err.Message = env.Body
		default:
			err.Message = env.Message
		}
	}
	if err.Message == "" {
		err.Message = fallbackMessage(resp.StatusCode, body)
	}
	return err
}

// fallbackMessage describes a response whose body was not one of the envelopes, which happens
// when nothing behind the edge answered and it replied for them.
func fallbackMessage(status int, body []byte) string {
	if text := strings.TrimSpace(string(body)); text != "" {
		if len(text) > maxErrorBody {
			text = text[:maxErrorBody]
		}
		return text
	}
	if text := http.StatusText(status); text != "" {
		return text
	}
	return "request failed"
}

func headerInt(h http.Header, name string) int64 {
	value, err := strconv.ParseInt(h.Get(name), 10, 64)
	if err != nil {
		return 0
	}
	return value
}
