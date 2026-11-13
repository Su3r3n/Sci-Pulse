package registry

import _ "embed"

// OpenAPIYAML embeds the OpenAPI 3.0 specification for serving at /openapi.yaml.
//
//go:embed openapi.yaml
var OpenAPIYAML []byte
