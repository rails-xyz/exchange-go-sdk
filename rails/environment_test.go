package rails

import (
	"bytes"
	"fmt"
	"log/slog"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCredentialsAreRedacted(t *testing.T) {
	creds := Credentials{APIKey: "my-key", APISecret: "hunter2"}
	var logged bytes.Buffer
	slog.New(slog.NewJSONHandler(&logged, nil)).Info("x", "creds", creds)

	for name, out := range map[string]string{
		"%v":   fmt.Sprintf("%v", creds),
		"%#v":  fmt.Sprintf("%#v", creds),
		"slog": logged.String(),
	} {
		assert.NotContains(t, out, "my-key", name)
		assert.NotContains(t, out, "hunter2", name)
	}
}

// A host missing from a preset stops its client from starting.
func TestPresetsNameEveryHost(t *testing.T) {
	prod, sandbox := reflect.ValueOf(Production()), reflect.ValueOf(Sandbox())
	for i := range prod.NumField() {
		name := prod.Type().Field(i).Name
		assert.NotEmpty(t, prod.Field(i).String(), "Production().%s", name)
		assert.NotEmpty(t, sandbox.Field(i).String(), "Sandbox().%s", name)
		assert.NotEqual(t, prod.Field(i).String(), sandbox.Field(i).String(), "%s is shared by both presets", name)
	}
}
