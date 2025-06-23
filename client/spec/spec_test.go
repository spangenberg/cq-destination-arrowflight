package spec

import (
	"testing"

	"github.com/cloudquery/codegen/jsonschema"
)

func TestSpec_JSONSchemaExtend(t *testing.T) {
	jsonschema.TestJSONSchema(t, JSONSchema, []jsonschema.TestCase{
		{
			Name: "missing addr",
			Spec: `{}`,
			Err:  true,
		},
		{
			Name: "empty addr",
			Spec: `{"addr": ""}`,
			Err:  true,
		},
		{
			Name: "null addr",
			Spec: `{"addr": null}`,
			Err:  true,
		},
		{
			Name: "integer addr",
			Spec: `{"addr": 123}`,
			Err:  true,
		},
		{
			Name: "non-empty addr",
			Spec: `{"addr": "abc"}`,
		},
	})
}
