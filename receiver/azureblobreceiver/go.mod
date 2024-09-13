module github.com/open-telemetry/opentelemetry-collector-contrib/receiver/azureblobreceiver

go 1.22.0

require (
	github.com/Azure/azure-event-hubs-go/v3 v3.6.2
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.14.0
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.7.0
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.4.0
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/sharedcomponent v0.109.0
	github.com/stretchr/testify v1.9.0
	go.opentelemetry.io/collector/component v0.109.1-0.20240911162712-6c2697c4453c
	go.opentelemetry.io/collector/config/configopaque v1.15.1-0.20240911162712-6c2697c4453c
	go.opentelemetry.io/collector/confmap v1.15.1-0.20240911162712-6c2697c4453c
	go.opentelemetry.io/collector/consumer v0.109.1-0.20240911162712-6c2697c4453c
	go.opentelemetry.io/collector/consumer/consumertest v0.109.1-0.20240911162712-6c2697c4453c
	go.opentelemetry.io/collector/otelcol/otelcoltest v0.109.1-0.20240911162712-6c2697c4453c
	go.opentelemetry.io/collector/pdata v1.15.1-0.20240911162712-6c2697c4453c
	go.opentelemetry.io/collector/receiver v0.109.1-0.20240911162712-6c2697c4453c
	go.uber.org/goleak v1.3.0
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.27.0
)

require (
	github.com/Azure/azure-amqp-common-go/v4 v4.2.0 // indirect
	github.com/Azure/azure-sdk-for-go v65.0.0+incompatible // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.10.0 // indirect
	github.com/Azure/go-amqp v1.0.2 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest v0.11.28 // indirect
	github.com/Azure/go-autorest/autorest/adal v0.9.21 // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.0 // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.1 // indirect
	github.com/Azure/go-autorest/logger v0.2.1 // indirect
	github.com/Azure/go-autorest/tracing v0.6.0 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.2.2 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/devigned/tab v0.1.1 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-viper/mapstructure/v2 v2.1.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt/v4 v4.5.0 // indirect
	github.com/golang-jwt/jwt/v5 v5.2.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.22.0 // indirect
	github.com/hashicorp/go-version v1.7.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/knadh/koanf/maps v0.1.1 // indirect
	github.com/knadh/koanf/providers/confmap v0.1.0 // indirect
	github.com/knadh/koanf/v2 v2.1.1 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/mapstructure v1.5.1-0.20231216201459-8508981c8b6c // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_golang v1.20.3 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.59.1 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/shirou/gopsutil/v4 v4.24.8 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/sirupsen/logrus v1.6.0 // indirect
	github.com/spf13/cobra v1.8.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.opentelemetry.io/collector v0.109.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/component/componentprofiles v0.109.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/component/componentstatus v0.109.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.109.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/confmap/provider/envprovider v1.15.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/confmap/provider/fileprovider v1.15.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/confmap/provider/httpprovider v0.109.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/confmap/provider/yamlprovider v0.109.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/connector v0.109.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/connector/connectorprofiles v0.109.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/consumer/consumerprofiles v0.109.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/exporter v0.109.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/exporter/exporterprofiles v0.109.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/extension v0.109.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/extension/extensioncapabilities v0.109.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/featuregate v1.15.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/internal/globalgates v0.109.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/otelcol v0.109.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.109.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/pdata/testdata v0.109.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/processor v0.109.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/processor/processorprofiles v0.109.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/receiver/receiverprofiles v0.109.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/semconv v0.109.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/collector/service v0.109.1-0.20240911162712-6c2697c4453c // indirect
	go.opentelemetry.io/contrib/config v0.9.0 // indirect
	go.opentelemetry.io/contrib/propagators/b3 v1.29.0 // indirect
	go.opentelemetry.io/otel v1.30.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp v0.5.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.30.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.30.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.29.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.29.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.29.0 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.52.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutlog v0.5.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.30.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.29.0 // indirect
	go.opentelemetry.io/otel/log v0.5.0 // indirect
	go.opentelemetry.io/otel/metric v1.30.0 // indirect
	go.opentelemetry.io/otel/sdk v1.30.0 // indirect
	go.opentelemetry.io/otel/sdk/log v0.5.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.30.0 // indirect
	go.opentelemetry.io/otel/trace v1.30.0 // indirect
	go.opentelemetry.io/proto/otlp v1.3.1 // indirect
	golang.org/x/crypto v0.27.0 // indirect
	golang.org/x/exp v0.0.0-20240506185415-9bf2ced13842 // indirect
	golang.org/x/net v0.29.0 // indirect
	golang.org/x/sys v0.25.0 // indirect
	golang.org/x/text v0.18.0 // indirect
	gonum.org/v1/gonum v0.15.1 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240903143218-8af14fe29dc1 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240903143218-8af14fe29dc1 // indirect
	google.golang.org/grpc v1.66.1 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/open-telemetry/opentelemetry-collector-contrib/internal/sharedcomponent => ../../internal/sharedcomponent

retract (
	v0.76.2
	v0.76.1
	v0.65.0
)
