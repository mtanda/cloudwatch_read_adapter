module github.com/mtanda/cloudwatch_read_adapter

go 1.12

require (
	github.com/alecthomas/units v0.0.0-20151022065526-2efee857e7cf
	github.com/aws/aws-sdk-go v1.16.30
	github.com/beorn7/perks v1.0.0
	github.com/cespare/xxhash v1.1.0
	github.com/go-kit/kit v0.8.0
	github.com/go-logfmt/logfmt v0.4.0
	github.com/go-stack/stack v1.8.0
	github.com/gogo/protobuf v1.2.1
	github.com/golang/protobuf v1.3.1
	github.com/golang/snappy v0.0.1
	github.com/grpc-ecosystem/grpc-gateway v1.8.5
	github.com/jmespath/go-jmespath v0.0.0-20180206201540-c2b33e8439af
	github.com/kr/logfmt v0.0.0-20140226030751-b84e30acd515
	github.com/matttproud/golang_protobuf_extensions v1.0.1
	github.com/oklog/ulid v1.3.1
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v1.0.0
	github.com/prometheus/client_model v0.0.0-20190129233127-fd36f4220a90
	github.com/prometheus/common v0.4.1
	github.com/prometheus/procfs v0.0.2
	github.com/prometheus/prometheus v0.0.0-20190709094220-4ef66003d985
	github.com/prometheus/tsdb v0.9.1
	golang.org/x/net v0.0.0-20190403144856-b630fd6fe46b
	golang.org/x/sync v0.0.0-20190227155943-e225da77a7e6
	golang.org/x/sys v0.0.0-20190403152447-81d4e9dc473e
	golang.org/x/text v0.3.1-0.20180805044716-cb6730876b98
	google.golang.org/genproto v0.0.0-20190307195333-5fe7a883aa19
	google.golang.org/grpc v1.19.1
	gopkg.in/yaml.v2 v2.2.2
)

replace github.com/oklog/ulid v0.0.0-20190509100159-be3bccf06dda => github.com/oklog/ulid/v2 v2.0.2
