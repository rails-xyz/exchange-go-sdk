package rails

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
