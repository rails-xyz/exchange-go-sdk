package rails

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// modulePath is this module's import path, which is how the SDK finds itself in the build info
// of whatever program embeds it.
const modulePath = "github.com/rails-xyz/exchange-go-sdk"

// devVersion is reported when no released version applies: the SDK's own tests and examples, a
// replace directive, or a build carrying no module information.
const devVersion = "(devel)"

// Version is the SDK release that was built in. It is read from the consuming program's build
// info rather than hardcoded, so a tag can never disagree with what the code reports.
func Version() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return devVersion
	}
	for _, dep := range info.Deps {
		if dep.Path != modulePath {
			continue
		}
		if dep.Replace != nil {
			if dep.Replace.Version != "" {
				return dep.Replace.Version
			}
			return devVersion // a directory replacement: local source, whatever go.mod required
		}
		if dep.Version != "" {
			return dep.Version
		}
		break
	}
	return devVersion
}

// UserAgent identifies this SDK on every HTTP request and on the WebSocket upgrade.
func UserAgent() string {
	return fmt.Sprintf("exchange-go-sdk/%s (%s; %s/%s)",
		Version(), runtime.Version(), runtime.GOOS, runtime.GOARCH)
}
