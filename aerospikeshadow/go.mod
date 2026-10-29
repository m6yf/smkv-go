module github.com/m6yf/smkv-go/aerospikeshadow

go 1.23.0

require (
	github.com/aerospike/aerospike-client-go v4.5.2+incompatible
	github.com/aerospike/aerospike-client-go/v8 v8.5.0
	github.com/m6yf/smkv-go v0.1.1
)

require (
	github.com/kr/text v0.2.0 // indirect
	github.com/onsi/ginkgo v1.16.5 // indirect
	github.com/rogpeppe/go-internal v1.13.1 // indirect
	github.com/wadey/gocovmerge v0.0.0-20160331181800-b5bfa59ec0ad // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	golang.org/x/sync v0.16.0 // indirect
	golang.org/x/tools v0.36.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// The client in this repository's root; ignored by modules that depend on this one.
replace github.com/m6yf/smkv-go => ../
