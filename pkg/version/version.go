// Package version resolves the binary's version string for -version flags.
package version

import "runtime/debug"

// value is set at build time via:
//
//	-ldflags "-X github.com/ChaosHour/go-parse/pkg/version.value=v1.2.3"
var value = ""

// String returns the release version: the ldflags-injected value if set,
// otherwise the module version recorded by `go install pkg@version`,
// otherwise "dev".
func String() string {
	if value != "" {
		return value
	}
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return "dev"
}
