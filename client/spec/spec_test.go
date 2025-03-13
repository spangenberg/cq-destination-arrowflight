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
		{
			Name: "zero batch_size",
			Spec: `{"addr": "abc", "batch_size": 0}`,
			Err:  true,
		},
		{
			Name: "negative batch_size",
			Spec: `{"addr": "abc", "batch_size": -1}`,
			Err:  true,
		},
		{
			Name: "float batch_size",
			Spec: `{"addr": "abc", "batch_size": 1.5}`,
			Err:  true,
		},
		{
			Name: "null batch_size",
			Spec: `{"addr": "abc", "batch_size": null}`,
			Err:  true,
		},
		{
			Name: "string batch_size",
			Spec: `{"addr": "abc", "batch_size": "123"}`,
			Err:  true,
		},
		{
			Name: "proper batch_size",
			Spec: `{"addr": "abc", "batch_size": 123}`,
		},
		{
			Name: "zero batch_size_bytes",
			Spec: `{"addr": "abc", "batch_size_bytes": 0}`,
			Err:  true,
		},
		{
			Name: "negative batch_size_bytes",
			Spec: `{"addr": "abc", "batch_size_bytes": -1}`,
			Err:  true,
		},
		{
			Name: "float batch_size_bytes",
			Spec: `{"addr": "abc", "batch_size_bytes": 1.5}`,
			Err:  true,
		},
		{
			Name: "null batch_size_bytes",
			Spec: `{"addr": "abc", "batch_size_bytes": null}`,
			Err:  true,
		},
		{
			Name: "string batch_size_bytes",
			Spec: `{"addr": "abc", "batch_size_bytes": "123"}`,
			Err:  true,
		},
		{
			Name: "proper batch_size_bytes",
			Spec: `{"addr": "abc", "batch_size_bytes": 123}`,
		},
		// batch_timeout is tested in configtype package, test only null & empty here
		{
			Name: "empty batch_timeout",
			Spec: `{"addr": "abc", "batch_timeout": ""}`,
			Err:  true,
		},
		{
			Name: "null batch_timeout",
			Spec: `{"addr": "abc", "batch_timeout": null}`,
			Err:  true,
		},
	})
}
