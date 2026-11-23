module github.com/nuroqzavitex/smarthire/services/application

go 1.27

replace github.com/nuroqzavitex/smarthire/pkg => ../../pkg

replace github.com/nuroqzavitex/smarthire/gen/go => ../../gen/go

require (
	github.com/exaring/otelpgx v0.13.0
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.12.0
	github.com/nuroqzavitex/smarthire/gen/go v0.0.0-00010101000000-000000000000
	github.com/nuroqzavitex/smarthire/pkg v0.0.0-00010101000000-000000000000
	github.com/sony/gobreaker/v2 v2.4.0
	github.com/stretchr/testify v1.12.1
	google.golang.org/grpc v1.85.0
	google.golang.org/protobuf v1.36.12
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.12-20260825204119-511051f7f437.2 // indirect
	github.com/MicahParks/jwkset v0.11.4 // indirect
	github.com/MicahParks/keyfunc/v3 v3.9.1 // indirect
	github.com/caarlos0/env/v11 v11.4.2 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.33.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/oklog/ulid/v2 v2.1.2 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.3 // indirect
	github.com/twmb/franz-go v1.22.3 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.15.1 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.47.0 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/trace v1.47.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20261104224025-d1e00e9f33ab // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261019180522-ce908d29299b // indirect
)
