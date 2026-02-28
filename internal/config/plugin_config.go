package config

type PluginConfig struct {
	Transforms []PluginTransformSpec `mapstructure:"transforms"`
	Adapters   []PluginAdapterSpec   `mapstructure:"adapters"`
	DLQ        *PluginSpec           `mapstructure:"dlq"`
	Checkpoint *PluginSpec           `mapstructure:"checkpoint"`
}

type PluginTransformSpec struct {
	Name     string         `mapstructure:"name"`
	Path     string         `mapstructure:"path"`
	Config   map[string]any `mapstructure:"config"`
	Encoding string         `mapstructure:"encoding"` // "json" (default) or "protobuf"
	Scope    any            `mapstructure:"scope"`    // "global" or map with "adapter" key
}

type PluginAdapterSpec struct {
	Name   string         `mapstructure:"name"`
	Path   string         `mapstructure:"path"`
	Config map[string]any `mapstructure:"config"`
}

type PluginSpec struct {
	Path   string         `mapstructure:"path"`
	Config map[string]any `mapstructure:"config"`
}
