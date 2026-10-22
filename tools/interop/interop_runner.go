// Copyright 2025 gRPC authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Self-contained single-file Go interop test runner driven by tools/interop/interop.json.
// Uses only the Go standard library and contains zero hardcoded language names.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ReportsConfig struct {
	DockerBuildXML   string `json:"docker_build_xml"`
	TestsXML         string `json:"tests_xml"`
	MatrixXML        string `json:"matrix_xml"`
	ManualServerCmds string `json:"manual_server_cmds"`
	ManualClientCmds string `json:"manual_client_cmds"`
}

type DefaultsConfig struct {
	DockerWorkdirRoot             string        `json:"docker_workdir_root"`
	DefaultServerPort             int           `json:"default_server_port"`
	ProdServerPort                int           `json:"prod_server_port"`
	TestTimeoutSeconds            int           `json:"test_timeout_seconds"`
	ServerTimeoutSeconds          int           `json:"server_timeout_seconds"`
	BuildImageTimeoutSeconds      int           `json:"build_image_timeout_seconds"`
	HTTP2ServerHealthTimeoutSecs  int           `json:"http2_server_health_timeout_seconds"`
	FlakeRetries                  int           `json:"flake_retries"`
	TimeoutRetries                int           `json:"timeout_retries"`
	MatrixFlakeRetries            int           `json:"matrix_flake_retries"`
	DefaultServiceAccountKeyFile  string        `json:"default_service_account_key_file"`
	DefaultServiceAccount         string        `json:"default_service_account"`
	OAuthScope                    string        `json:"oauth_scope"`
	CloudToCloudServerHostOverr   string        `json:"cloud_to_cloud_server_host_override"`
	DockerPropagateEnvFile        string        `json:"docker_propagate_env_file"`
	LegacyImageBuilderScript      string        `json:"legacy_image_builder_script"`
	Reports                       ReportsConfig `json:"reports"`
}

type SecurityOption struct {
	ClientFlags      []string    `json:"client_flags"`
	ServerFlags      []string    `json:"server_flags"`
	SupportedClients interface{} `json:"supported_clients"`
	SupportedServers interface{} `json:"supported_servers"`
}

type SuiteConfig struct {
	Description            string   `json:"description"`
	ShortnameTemplate      string   `json:"shortname_template"`
	DefaultTransportSec    string   `json:"default_transport_security"`
	ClientExtraFlags       []string `json:"client_extra_flags"`
	SkipCloudToCloudSrv    []string `json:"skip_cloud_to_cloud_servers"`
	SupportedHTTP2Clients  []string `json:"supported_http2_clients"`
	JUnitPackage           string   `json:"junit_package"`
}

type StandardTestCase struct {
	Name                    string   `json:"name"`
	CloudToProdEligible     bool     `json:"cloud_to_prod_eligible"`
	ExtraCloudToCloudFlags  []string `json:"extra_cloud_to_cloud_flags"`
}

type AuthTestCase struct {
	Name                                 string   `json:"name"`
	TransportSecurity                    string   `json:"transport_security"`
	RequiresComputeEngineCreds           bool     `json:"requires_compute_engine_creds"`
	RequiresComputeEngineCredsLanguages  []string `json:"requires_compute_engine_creds_languages"`
	UsesServiceAccountKey                bool     `json:"uses_service_account_key"`
	UsesServiceAccountKeyWhenFlag        string   `json:"uses_service_account_key_when_flag"`
	UsesOAuthScope                       bool     `json:"uses_oauth_scope"`
	UsesDefaultServiceAccount            bool     `json:"uses_default_service_account"`
	UsesDefaultServiceAccountLanguages   []string `json:"uses_default_service_account_languages"`
}

func (a AuthTestCase) NeedsComputeEngineCreds(lang string) bool {
	if a.RequiresComputeEngineCreds {
		return true
	}
	for _, l := range a.RequiresComputeEngineCredsLanguages {
		if l == lang {
			return true
		}
	}
	return false
}

func (a AuthTestCase) NeedsDefaultServiceAccount(lang string) bool {
	if a.UsesDefaultServiceAccount {
		return true
	}
	for _, l := range a.UsesDefaultServiceAccountLanguages {
		if l == lang {
			return true
		}
	}
	return false
}

type HTTP2ClientTestCase struct {
	Name string `json:"name"`
}

type HTTP2ServerTestCase struct {
	Name                      string `json:"name"`
	ClientKind                string `json:"client_kind"`
	GRPCClientTestCase        string `json:"grpc_client_test_case"`
	TransportSecurityOverride string `json:"transport_security_override"`
}

type TestCasesSection struct {
	Standard    []StandardTestCase    `json:"standard"`
	Auth        []AuthTestCase        `json:"auth"`
	HTTP2Client []HTTP2ClientTestCase `json:"http2_client"`
	HTTP2Server []HTTP2ServerTestCase `json:"http2_server"`
}

type CommandSpec struct {
	Command     []string `json:"command"`
	ArgsMode    string   `json:"args_mode"`
	ArgTemplate string   `json:"arg_template"`
	PortRegex   string   `json:"port_regex"`
}

func (c *CommandSpec) Render(args []string) ([]string, bool) {
	if c == nil {
		return nil, false
	}
	mode := c.ArgsMode
	if mode == "" {
		mode = "append"
	}
	switch mode {
	case "append":
		out := make([]string, 0, len(c.Command)+len(args))
		out = append(out, c.Command...)
		out = append(out, args...)
		return out, true
	case "joined_flag":
		joined := strings.Join(args, " ")
		tmpl := c.ArgTemplate
		if tmpl == "" {
			tmpl = `--args="{args}"`
		}
		formatted := strings.ReplaceAll(tmpl, "{args}", joined)
		out := make([]string, 0, len(c.Command)+1)
		out = append(out, c.Command...)
		out = append(out, formatted)
		return out, true
	case "ignore_args":
		out := make([]string, len(c.Command))
		copy(out, c.Command)
		return out, true
	case "extract_port_template":
		pattern := c.PortRegex
		if pattern == "" {
			pattern = `--server_port=(\d+)`
		}
		re := regexp.MustCompile(pattern)
		for _, a := range args {
			m := re.FindStringSubmatch(a)
			if len(m) > 1 {
				cmdline := strings.ReplaceAll(c.ArgTemplate, "{port}", m[1])
				return []string{cmdline}, true
			}
		}
		return nil, false
	default:
		return nil, false
	}
}

type DockerBuildSpec struct {
	BaseName           string            `json:"base_name"`
	CurrentVersionFile string            `json:"current_version_file"`
	DockerfileDir      string            `json:"dockerfile_dir"`
	BuildScript        string            `json:"build_script"`
	RepoMount          string            `json:"repo_mount"`
	Workdir            string            `json:"workdir"`
	Env                map[string]string `json:"env"`
	BuildCommands      []string          `json:"build_commands"`
}

type HealthcheckSpec struct {
	CmdTemplate        string `json:"cmd_template"`
	Interval           string `json:"interval"`
	Retries            int    `json:"retries"`
	Timeout            string `json:"timeout"`
	WaitTimeoutSeconds int    `json:"wait_timeout_seconds"`
}

type ServerDockerConfig struct {
	PortCountFromSuite string           `json:"port_count_from_suite"`
	PortCount          int              `json:"port_count"`
	Healthcheck        *HealthcheckSpec `json:"healthcheck"`
}

type LanguageConfig struct {
	Name                           string              `json:"-"`
	Safename                       string              `json:"safename"`
	LegacyClass                    string              `json:"legacy_class"`
	Repo                           string              `json:"repo"`
	IncludedInAllClients           bool                `json:"included_in_all_clients"`
	SupportsServer                 bool                `json:"supports_server"`
	DockerSupported                bool                `json:"docker_supported"`
	IsHTTP2Target                  bool                `json:"is_http2_target"`
	AuthCredentialsMode            string              `json:"auth_credentials_mode"`
	ClientCwd                      *string             `json:"client_cwd"`
	ServerCwd                      *string             `json:"server_cwd"`
	HTTP2Cwd                       *string             `json:"http2_cwd"`
	GlobalEnv                      map[string]string   `json:"global_env"`
	CloudToProdEnv                 map[string]string   `json:"cloud_to_prod_env"`
	ClientCmd                      *CommandSpec        `json:"client_cmd"`
	ServerCmd                      *CommandSpec        `json:"server_cmd"`
	HTTP2ClientCmd                 *CommandSpec        `json:"http2_client_cmd"`
	ClientTestCases                []string            `json:"client_test_cases"`
	ServerTestCases                []string            `json:"server_test_cases"`
	UnimplementedClientCases       []string            `json:"unimplemented_client_cases"`
	UnimplementedServerCases       []string            `json:"unimplemented_server_cases"`
	ServerUnimplementedClientCases []string            `json:"server_unimplemented_client_cases"`
	DockerBuild                    *DockerBuildSpec    `json:"docker_build"`
	ServerDockerConfig             *ServerDockerConfig `json:"server_docker_config"`

	ExpandedUnimplClient       []string `json:"-"`
	ExpandedUnimplServer       []string `json:"-"`
	ExpandedServerUnimplClient []string `json:"-"`
}

type OrderedEntry struct {
	Key   string
	Value string
}

type OrderedMap []OrderedEntry

func (m *OrderedMap) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	t, err := dec.Token()
	if err != nil {
		return err
	}
	if t == nil {
		return nil
	}
	if t != json.Delim('{') {
		return fmt.Errorf("expected '{', got %v", t)
	}
	var entries []OrderedEntry
	for dec.More() {
		kTok, err := dec.Token()
		if err != nil {
			return err
		}
		k := kTok.(string)
		var v string
		if err := dec.Decode(&v); err != nil {
			return err
		}
		entries = append(entries, OrderedEntry{Key: k, Value: v})
	}
	*m = entries
	return nil
}

type MatrixProfile struct {
	DefaultImage         string     `json:"default_image"`
	Workdir              string     `json:"workdir"`
	InnerCommandTemplate string     `json:"inner_command_template"`
	Env                  OrderedMap `json:"env"`
	BashLogin            bool       `json:"bash_login"`
	ContainerNames       []string   `json:"container_names"`
}

func (p MatrixProfile) RenderCommands(servers []string, testCases []string) []string {
	var envParts []string
	for _, kv := range p.Env {
		envParts = append(envParts, fmt.Sprintf("-e %s=%s", kv.Key, kv.Value))
	}
	envStr := ""
	if len(envParts) > 0 {
		envStr = " " + strings.Join(envParts, " ")
	}
	bashCmd := "bash -c"
	if p.BashLogin {
		bashCmd = "bash -l -c"
	}
	var lines []string
	idx := 0
	for _, srv := range servers {
		for _, tc := range testCases {
			nameStr := ""
			if idx < len(p.ContainerNames) && p.ContainerNames[idx] != "" {
				nameStr = " --name " + p.ContainerNames[idx]
			}
			inner := strings.ReplaceAll(p.InnerCommandTemplate, "{server_host}", srv)
			inner = strings.ReplaceAll(inner, "{test_case}", tc)
			line := fmt.Sprintf(`docker run -i --rm=true%s -w %s --net=host%s $docker_image %s "%s"`,
				envStr, p.Workdir, nameStr, bashCmd, inner)
			lines = append(lines, line)
			idx++
		}
	}
	return lines
}

type MatrixSection struct {
	DefaultDockerPath string                        `json:"default_docker_path"`
	DefaultServerHost string                        `json:"default_server_host"`
	Servers           []string                      `json:"servers"`
	TestCases         []string                      `json:"test_cases"`
	LanguageOrder     []string                      `json:"language_order"`
	Runtimes          map[string]DockerBuildSpec    `json:"runtimes"`
	TestcaseProfiles  map[string]MatrixProfile      `json:"testcase_profiles"`
}

type InteropConfig struct {
	SchemaVersion          string                      `json:"schema_version"`
	Defaults               DefaultsConfig              `json:"defaults"`
	ProdServers            map[string]string           `json:"prod_servers"`
	TransportSecurity      map[string]SecurityOption   `json:"transport_security"`
	CustomCredentialsTypes map[string]SecurityOption   `json:"custom_credentials_types"`
	TestCaseGroups         map[string][]string         `json:"test_case_groups"`
	Suites                 map[string]SuiteConfig      `json:"suites"`
	TestCases              TestCasesSection            `json:"test_cases"`
	Languages              map[string]*LanguageConfig  `json:"languages"`
	Matrix                 MatrixSection               `json:"matrix"`

	LanguageOrder          []string                    `json:"-"`
	BuiltinGroups          map[string][]string         `json:"-"`
	HTTP2Target            *LanguageConfig             `json:"-"`
	HTTP2ServerTarget      *LanguageConfig             `json:"-"`
	ActiveServers          []string                    `json:"-"`
	ServerCLIChoices       []string                    `json:"-"`
	StandardMap            map[string]StandardTestCase `json:"-"`
	AuthMap                map[string]AuthTestCase     `json:"-"`
	HTTP2ServerMap         map[string]HTTP2ServerTestCase `json:"-"`
	HTTP2ServerGRPCMap     map[string]string           `json:"-"`
	HTTP2ServerGRPCCases   []string                    `json:"-"`
}

type JobSpec struct {
	Shortname      string            `json:"shortname"`
	Cmdline        []string          `json:"cmdline"`
	Cwd            *string           `json:"cwd"`
	Environ        map[string]string `json:"environ"`
	TimeoutSeconds int               `json:"timeout_seconds"`
	FlakeRetries   int               `json:"flake_retries"`
	TimeoutRetries int               `json:"timeout_retries"`
	ContainerName  string            `json:"container_name,omitempty"`
	Tag            string            `json:"tag,omitempty"`
}

type SkippedRecord struct {
	Suite    string `json:"suite"`
	Client   string `json:"client"`
	Server   string `json:"server"`
	TestCase string `json:"test_case"`
	Reason   string `json:"reason"`
}

type PlanJSON struct {
	Languages   []string           `json:"languages"`
	Servers     []string           `json:"servers"`
	HTTP2Client []string           `json:"http2_clients"`
	BuildJobs   []JobSpec          `json:"build_jobs"`
	ServerSpecs map[string]JobSpec `json:"server_specs"`
	ClientJobs  []JobSpec          `json:"client_jobs"`
	Skipped     []SkippedRecord    `json:"skipped_tests"`
}

func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func extractJSONKeysInOrder(raw []byte, section string) []string {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		return nil
	}
	secBytes, ok := top[section]
	if !ok {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(secBytes))
	t, err := dec.Token()
	if err != nil || t != json.Delim('{') {
		return nil
	}
	var keys []string
	for dec.More() {
		kTok, err := dec.Token()
		if err != nil {
			break
		}
		key, ok := kTok.(string)
		if !ok {
			break
		}
		keys = append(keys, key)
		var val json.RawMessage
		if err := dec.Decode(&val); err != nil {
			break
		}
	}
	return keys
}

func loadConfig(path string) (*InteropConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg InteropConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	cfg.LanguageOrder = extractJSONKeysInOrder(data, "languages")

	var stdNames, authNames, h2cNames, h2sNames []string
	cfg.StandardMap = make(map[string]StandardTestCase)
	for _, c := range cfg.TestCases.Standard {
		stdNames = append(stdNames, c.Name)
		cfg.StandardMap[c.Name] = c
	}
	cfg.AuthMap = make(map[string]AuthTestCase)
	for _, c := range cfg.TestCases.Auth {
		authNames = append(authNames, c.Name)
		cfg.AuthMap[c.Name] = c
	}
	for _, c := range cfg.TestCases.HTTP2Client {
		h2cNames = append(h2cNames, c.Name)
	}
	cfg.HTTP2ServerMap = make(map[string]HTTP2ServerTestCase)
	cfg.HTTP2ServerGRPCMap = make(map[string]string)
	for _, c := range cfg.TestCases.HTTP2Server {
		h2sNames = append(h2sNames, c.Name)
		cfg.HTTP2ServerMap[c.Name] = c
		if c.ClientKind == "grpc_client" && c.GRPCClientTestCase != "" {
			cfg.HTTP2ServerGRPCMap[c.Name] = c.GRPCClientTestCase
			cfg.HTTP2ServerGRPCCases = append(cfg.HTTP2ServerGRPCCases, c.Name)
		}
	}

	cfg.BuiltinGroups = map[string][]string{
		"standard_test_cases":              stdNames,
		"standard_test_cases_except_first": stdNames[1:],
		"auth_test_cases":                  authNames,
		"http2_client_test_cases":          h2cNames,
		"http2_server_test_cases":          h2sNames,
	}

	expand := func(refs []string) []string {
		var out []string
		for _, r := range refs {
			if strings.HasPrefix(r, "@") {
				g := r[1:]
				if grp, ok := cfg.TestCaseGroups[g]; ok {
					out = append(out, grp...)
				} else if grp, ok := cfg.BuiltinGroups[g]; ok {
					out = append(out, grp...)
				}
			} else {
				out = append(out, r)
			}
		}
		return out
	}

	allClientUniverse := append(append([]string{}, stdNames...), authNames...)
	allClientUniverse = append(allClientUniverse, cfg.HTTP2ServerGRPCCases...)

	for _, lname := range cfg.LanguageOrder {
		lcfg := cfg.Languages[lname]
		lcfg.Name = lname
		if len(lcfg.UnimplementedClientCases) > 0 {
			lcfg.ExpandedUnimplClient = expand(lcfg.UnimplementedClientCases)
		} else if len(lcfg.ClientTestCases) > 0 {
			pos := expand(lcfg.ClientTestCases)
			for _, u := range allClientUniverse {
				if !containsStr(pos, u) {
					lcfg.ExpandedUnimplClient = append(lcfg.ExpandedUnimplClient, u)
				}
			}
		}
		if len(lcfg.UnimplementedServerCases) > 0 {
			lcfg.ExpandedUnimplServer = expand(lcfg.UnimplementedServerCases)
		} else if len(lcfg.ServerTestCases) > 0 {
			pos := expand(lcfg.ServerTestCases)
			for _, u := range stdNames {
				if !containsStr(pos, u) {
					lcfg.ExpandedUnimplServer = append(lcfg.ExpandedUnimplServer, u)
				}
			}
		}
		if len(lcfg.ServerUnimplementedClientCases) > 0 {
			lcfg.ExpandedServerUnimplClient = expand(lcfg.ServerUnimplementedClientCases)
		} else {
			lcfg.ExpandedServerUnimplClient = lcfg.ExpandedUnimplClient
		}
		if lcfg.ServerDockerConfig != nil && lcfg.ServerDockerConfig.PortCountFromSuite == "http2_server" {
			lcfg.ServerDockerConfig.PortCount = len(h2sNames)
		}
		if lcfg.IsHTTP2Target {
			cfg.HTTP2Target = lcfg
			srvCopy := *lcfg
			srvCopy.ExpandedUnimplClient = lcfg.ExpandedServerUnimplClient
			cfg.HTTP2ServerTarget = &srvCopy
		} else if lcfg.SupportsServer {
			cfg.ServerCLIChoices = append(cfg.ServerCLIChoices, lname)
			if len(lcfg.ServerTestCases) > 0 {
				cfg.ActiveServers = append(cfg.ActiveServers, lname)
			}
		}
	}
	return &cfg, nil
}

func isAllowed(allowed interface{}, name string) bool {
	if allowed == nil {
		return true
	}
	if s, ok := allowed.(string); ok && s == "all" {
		return true
	}
	if arr, ok := allowed.([]interface{}); ok {
		for _, item := range arr {
			if fmt.Sprint(item) == name {
				return true
			}
		}
		return false
	}
	return false
}

func containsStr(slice []string, target string) bool {
	for _, s := range slice {
		if s == target {
			return true
		}
	}
	return false
}

func dockerRunCmdline(cmdline []string, image string, dockerArgs []string, cwd *string, environ map[string]string, workdirRoot string) []string {
	out := []string{"docker", "run", "-i", "--rm=true"}
	if len(environ) > 0 {
		keys := make([]string, 0, len(environ))
		for k := range environ {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			out = append(out, "-e", fmt.Sprintf("%s=%s", k, environ[k]))
		}
	}
	wdir := workdirRoot
	if cwd != nil && *cwd != "" {
		wdir = filepath.Join(wdir, *cwd)
	}
	out = append(out, "-w", wdir)
	out = append(out, dockerArgs...)
	out = append(out, image)
	out = append(out, cmdline...)
	return out
}

func manualCmdline(dockerCmdline []string, dockerImage string) string {
	var parts []string
	for _, item := range dockerCmdline {
		if strings.HasPrefix(item, "--name=") {
			continue
		}
		if item == dockerImage {
			item = "$docker_image"
		}
		item = strings.ReplaceAll(item, `"`, `\"`)
		if strings.ContainsAny(item, " \t\n\r") {
			item = fmt.Sprintf(`"%s"`, item)
		}
		parts = append(parts, item)
	}
	return strings.Join(parts, " ")
}

type CLIArgs struct {
	Languages                     []string
	Servers                       []string
	Jobs                          int
	CloudToProd                   bool
	CloudToProdAuth               bool
	GoogleDefaultCredsUseKeyFile  bool
	ProdServers                   []string
	OverrideServers               [][2]string
	ServiceAccountKeyFile         string
	DefaultServiceAccount         string
	Travis                        bool
	Verbose                       bool
	UseDocker                     bool
	AllowFlakes                   bool
	ManualRun                     bool
	HTTP2Interop                  bool
	HTTP2ServerInterop            bool
	TransportSecurity             string
	CustomCredentialsType         []string
	SkipComputeEngineCreds        bool
	BQResultTable                 string
	ListLanguages                 bool
	ListTests                     bool
	ExplainMatrix                 string
	DryRun                        bool
	ShowDockerfile                string
	DirectDockerBuild             bool
	DumpPlanJSON                  bool
	MatrixTestcaseProfile         string
}

func parseCLIArgs(argv []string, cfg *InteropConfig) (*CLIArgs, error) {
	res := &CLIArgs{
		Languages:             []string{"all"},
		Servers:               []string{},
		Jobs:                  runtime.NumCPU(),
		ProdServers:           []string{"default"},
		ServiceAccountKeyFile: cfg.Defaults.DefaultServiceAccountKeyFile,
		DefaultServiceAccount: cfg.Defaults.DefaultServiceAccount,
		TransportSecurity:     "tls",
		CustomCredentialsType: []string{"tls", "google_default_credentials", "compute_engine_channel_creds"},
	}

	seenLang := false
	seenSrv := false
	seenProd := false
	seenCred := false

	for i := 0; i < len(argv); i++ {
		arg := argv[i]
		key := arg
		val := ""
		hasEq := false
		if idx := strings.IndexByte(arg, '='); idx != -1 {
			key = arg[:idx]
			val = arg[idx+1:]
			hasEq = true
		}

		collectMulti := func() []string {
			if hasEq {
				return []string{val}
			}
			var vals []string
			for i+1 < len(argv) && !strings.HasPrefix(argv[i+1], "-") {
				i++
				vals = append(vals, argv[i])
			}
			return vals
		}

		switch key {
		case "-l", "--language":
			if !seenLang {
				seenLang = true
				res.Languages = nil
			}
			for _, v := range collectMulti() {
				if !containsStr(res.Languages, v) {
					res.Languages = append(res.Languages, v)
				}
			}
		case "-s", "--server":
			if !seenSrv {
				seenSrv = true
				res.Servers = nil
			}
			for _, v := range collectMulti() {
				if !containsStr(res.Servers, v) {
					res.Servers = append(res.Servers, v)
				}
			}
		case "--prod_servers":
			if !seenProd {
				seenProd = true
				res.ProdServers = nil
			}
			res.ProdServers = append(res.ProdServers, collectMulti()...)
		case "--custom_credentials_type":
			if !seenCred {
				seenCred = true
				res.CustomCredentialsType = nil
			}
			res.CustomCredentialsType = append(res.CustomCredentialsType, collectMulti()...)
		case "--override_server":
			if !hasEq {
				if i+1 >= len(argv) {
					return nil, fmt.Errorf("--override_server requires an argument")
				}
				i++
				val = argv[i]
			}
			eqIdx := strings.IndexByte(val, '=')
			if eqIdx <= 0 || !strings.Contains(val[eqIdx+1:], ":") {
				return nil, fmt.Errorf("invalid --override_server %q: expected SERVER=HOST:PORT", val)
			}
			res.OverrideServers = append(res.OverrideServers, [2]string{val[:eqIdx], val[eqIdx+1:]})
		case "-j", "--jobs":
			if !hasEq && i+1 < len(argv) {
				i++
				val = argv[i]
			}
			n, err := strconv.Atoi(val)
			if err != nil {
				return nil, err
			}
			res.Jobs = n
		case "--cloud_to_prod":
			res.CloudToProd = true
		case "--cloud_to_prod_auth":
			res.CloudToProdAuth = true
		case "--google_default_creds_use_key_file":
			res.GoogleDefaultCredsUseKeyFile = true
		case "--service_account_key_file":
			if !hasEq && i+1 < len(argv) {
				i++
				val = argv[i]
			}
			res.ServiceAccountKeyFile = val
		case "--default_service_account":
			if !hasEq && i+1 < len(argv) {
				i++
				val = argv[i]
			}
			res.DefaultServiceAccount = val
		case "-t", "--travis":
			res.Travis = true
		case "-v", "--verbose":
			res.Verbose = true
		case "--use_docker":
			res.UseDocker = true
		case "--allow_flakes":
			res.AllowFlakes = true
		case "--manual_run":
			res.ManualRun = true
		case "--http2_interop":
			res.HTTP2Interop = true
		case "--http2_server_interop":
			res.HTTP2ServerInterop = true
		case "--transport_security":
			if !hasEq && i+1 < len(argv) && !strings.HasPrefix(argv[i+1], "-") {
				i++
				val = argv[i]
			}
			res.TransportSecurity = val
		case "--skip_compute_engine_creds":
			res.SkipComputeEngineCreds = true
		case "--bq_result_table":
			if !hasEq && i+1 < len(argv) && !strings.HasPrefix(argv[i+1], "-") {
				i++
				val = argv[i]
			}
			res.BQResultTable = val
		case "--list-languages":
			res.ListLanguages = true
		case "--list-tests":
			res.ListTests = true
		case "--explain-matrix":
			if hasEq {
				res.ExplainMatrix = val
			} else if i+1 < len(argv) && !strings.HasPrefix(argv[i+1], "-") {
				i++
				res.ExplainMatrix = argv[i]
			} else {
				res.ExplainMatrix = "all"
			}
		case "--dry-run":
			res.DryRun = true
		case "--show-dockerfile":
			if !hasEq && i+1 < len(argv) {
				i++
				val = argv[i]
			}
			res.ShowDockerfile = val
		case "--direct-docker-build":
			res.DirectDockerBuild = true
		case "--dump-plan-json":
			res.DumpPlanJSON = true
		case "--matrix-testcase-profile":
			if !hasEq && i+1 < len(argv) {
				i++
				val = argv[i]
			}
			res.MatrixTestcaseProfile = val
		default:
			return nil, fmt.Errorf("unrecognized argument: %s", arg)
		}
	}
	return res, nil
}

func serverHasImplementedCases(cfg *InteropConfig, srv string, args *CLIArgs) bool {
	lcfg, ok := cfg.Languages[srv]
	if !ok || !lcfg.SupportsServer {
		return false
	}
	for _, tc := range cfg.TestCases.Standard {
		if !containsStr(lcfg.ExpandedUnimplServer, tc.Name) {
			return true
		}
	}
	if args.HTTP2Interop {
		skipH2 := cfg.Suites["http2_client"].SkipCloudToCloudSrv
		if !containsStr(skipH2, srv) {
			for _, tc := range cfg.TestCases.HTTP2Client {
				if !containsStr(lcfg.ExpandedUnimplServer, tc.Name) {
					return true
				}
			}
		}
	}
	return false
}

func resolveLanguagesAndServers(cfg *InteropConfig, args *CLIArgs) ([]*LanguageConfig, []string, []*LanguageConfig) {
	srvSet := make(map[string]bool)
	for _, s := range args.Servers {
		if s == "all" {
			for _, as := range cfg.ActiveServers {
				srvSet[as] = true
			}
		} else {
			srvSet[s] = true
		}
	}
	if args.TransportSecurity == "alts" {
		altsOpt := cfg.TransportSecurity["alts"]
		for s := range srvSet {
			if !isAllowed(altsOpt.SupportedServers, s) {
				delete(srvSet, s)
			}
		}
	}
	var servers []string
	for s := range srvSet {
		if serverHasImplementedCases(cfg, s, args) {
			servers = append(servers, s)
		}
	}
	sort.Strings(servers)

	langSet := make(map[string]bool)
	for _, l := range args.Languages {
		if l == "all" {
			for name, lcfg := range cfg.Languages {
				if !lcfg.IsHTTP2Target && lcfg.IncludedInAllClients {
					langSet[name] = true
				}
			}
		} else {
			langSet[l] = true
		}
	}
	if args.TransportSecurity == "alts" {
		altsOpt := cfg.TransportSecurity["alts"]
		for l := range langSet {
			if !isAllowed(altsOpt.SupportedClients, l) {
				delete(langSet, l)
			}
		}
	}
	var langNames []string
	for l := range langSet {
		langNames = append(langNames, l)
	}
	sort.Strings(langNames)
	var languages []*LanguageConfig
	for _, l := range langNames {
		languages = append(languages, cfg.Languages[l])
	}

	var h2Clients []*LanguageConfig
	if args.HTTP2ServerInterop {
		var h2Names []string
		for _, l := range cfg.Suites["http2_server"].SupportedHTTP2Clients {
			if containsStr(args.Languages, "all") || containsStr(args.Languages, l) {
				h2Names = append(h2Names, l)
			}
		}
		sort.Strings(h2Names)
		for _, l := range h2Names {
			h2Clients = append(h2Clients, cfg.Languages[l])
		}
	}
	return languages, servers, h2Clients
}

func authOptions(cfg *InteropConfig, lang *LanguageConfig, testCase string, googleDefaultUseKey bool, keyFile, defaultSA string) ([]string, map[string]string) {
	cmdargs := []string{}
	env := make(map[string]string)
	authSpec, ok := cfg.AuthMap[testCase]
	if !ok {
		return cmdargs, env
	}
	if authSpec.UsesServiceAccountKey {
		if lang.AuthCredentialsMode == "env" {
			env["GOOGLE_APPLICATION_CREDENTIALS"] = keyFile
		} else {
			cmdargs = append(cmdargs, "--service_account_key_file="+keyFile)
		}
	}
	if authSpec.UsesOAuthScope {
		cmdargs = append(cmdargs, "--oauth_scope="+cfg.Defaults.OAuthScope)
	}
	if authSpec.UsesServiceAccountKeyWhenFlag == "google_default_creds_use_key_file" && googleDefaultUseKey {
		env["GOOGLE_APPLICATION_CREDENTIALS"] = keyFile
	}
	if authSpec.NeedsDefaultServiceAccount(lang.Name) {
		cmdargs = append(cmdargs, "--default_service_account="+defaultSA)
	}
	return cmdargs, env
}

func buildCloudToProdJobSpec(cfg *InteropConfig, lang *LanguageConfig, testCase, nickname, host string, args *CLIArgs, dockerImage string, auth bool, transportSec string, manualLog *[]string) JobSpec {
	cmdargs := []string{
		fmt.Sprintf("--server_host=%s", host),
		fmt.Sprintf("--server_port=%d", cfg.Defaults.ProdServerPort),
		fmt.Sprintf("--test_case=%s", testCase),
	}
	credSpec := cfg.CustomCredentialsTypes[transportSec]
	cmdargs = append(cmdargs, credSpec.ClientFlags...)

	environ := make(map[string]string)
	for k, v := range lang.CloudToProdEnv {
		environ[k] = v
	}
	for k, v := range lang.GlobalEnv {
		environ[k] = v
	}
	if auth {
		aCmd, aEnv := authOptions(cfg, lang, testCase, args.GoogleDefaultCredsUseKeyFile, args.ServiceAccountKeyFile, args.DefaultServiceAccount)
		cmdargs = append(cmdargs, aCmd...)
		for k, v := range aEnv {
			environ[k] = v
		}
	}
	rendered, _ := lang.ClientCmd.Render(cmdargs)
	cmdline := []string{"bash", "-c", strings.Join(rendered, " ")}
	cwd := lang.ClientCwd
	containerName := ""
	if dockerImage != "" {
		containerName = fmt.Sprintf("interop_client_%s_%s", lang.Safename, newUUID())
		cmdline = dockerRunCmdline(cmdline, dockerImage, []string{"--net=host", "--name=" + containerName}, cwd, environ, cfg.Defaults.DockerWorkdirRoot)
		if manualLog != nil {
			if len(*manualLog) == 0 {
				*manualLog = append(*manualLog, fmt.Sprintf(`echo "Testing ${docker_image:=%s}"`, dockerImage))
			}
			*manualLog = append(*manualLog, manualCmdline(cmdline, dockerImage))
		}
		cwd = nil
		environ = nil
	}
	suiteName := "cloud_to_prod"
	if auth {
		suiteName = "cloud_to_prod_auth"
	}
	flakeRetries := 0
	timeoutRetries := 0
	if args.AllowFlakes {
		flakeRetries = cfg.Defaults.FlakeRetries
		timeoutRetries = cfg.Defaults.TimeoutRetries
	}
	return JobSpec{
		Shortname:      fmt.Sprintf("%s:%s:%s:%s:%s", suiteName, lang.Name, nickname, testCase, transportSec),
		Cmdline:        cmdline,
		Cwd:            cwd,
		Environ:        environ,
		TimeoutSeconds: cfg.Defaults.TestTimeoutSeconds,
		FlakeRetries:   flakeRetries,
		TimeoutRetries: timeoutRetries,
		ContainerName:  containerName,
	}
}

func buildCloudToCloudJobSpec(cfg *InteropConfig, lang *LanguageConfig, testCase, serverName, serverHost, serverPort string, args *CLIArgs, dockerImage, transportSec string, manualLog *[]string) JobSpec {
	var interopOnly []string
	interopOnly = append(interopOnly, cfg.Suites["cloud_to_cloud"].ClientExtraFlags...)
	secSpec := cfg.TransportSecurity[transportSec]
	interopOnly = append(interopOnly, secSpec.ClientFlags...)

	clientTestCase := testCase
	if mapped, ok := cfg.HTTP2ServerGRPCMap[testCase]; ok {
		clientTestCase = mapped
	}
	if stdCase, ok := cfg.StandardMap[testCase]; ok && len(stdCase.ExtraCloudToCloudFlags) > 0 {
		interopOnly = append(interopOnly, stdCase.ExtraCloudToCloudFlags...)
	}
	commonOptions := []string{
		fmt.Sprintf("--test_case=%s", clientTestCase),
		fmt.Sprintf("--server_host=%s", serverHost),
		fmt.Sprintf("--server_port=%s", serverPort),
	}

	var cmdline []string
	var cwd *string
	if _, isH2Srv := cfg.HTTP2ServerMap[testCase]; isH2Srv {
		if _, usesGRPC := cfg.HTTP2ServerGRPCMap[testCase]; usesGRPC {
			opts := append(append([]string{}, interopOnly...), commonOptions...)
			rendered, _ := lang.ClientCmd.Render(opts)
			cmdline = []string{"bash", "-c", strings.Join(rendered, " ")}
			cwd = lang.ClientCwd
		} else {
			rendered, _ := lang.HTTP2ClientCmd.Render(commonOptions)
			cmdline = []string{"bash", "-c", strings.Join(rendered, " ")}
			cwd = lang.HTTP2Cwd
		}
	} else {
		opts := append(append([]string{}, commonOptions...), interopOnly...)
		rendered, _ := lang.ClientCmd.Render(opts)
		cmdline = []string{"bash", "-c", strings.Join(rendered, " ")}
		cwd = lang.ClientCwd
	}

	environ := make(map[string]string)
	for k, v := range lang.GlobalEnv {
		environ[k] = v
	}
	containerName := ""
	if dockerImage != "" && lang.DockerSupported {
		containerName = fmt.Sprintf("interop_client_%s_%s", lang.Safename, newUUID())
		cmdline = dockerRunCmdline(cmdline, dockerImage, []string{"--net=host", "--name=" + containerName}, cwd, environ, cfg.Defaults.DockerWorkdirRoot)
		if manualLog != nil {
			if len(*manualLog) == 0 {
				*manualLog = append(*manualLog, fmt.Sprintf(`echo "Testing ${docker_image:=%s}"`, dockerImage))
			}
			*manualLog = append(*manualLog, manualCmdline(cmdline, dockerImage))
		}
		cwd = nil
	}
	flakeRetries := 0
	timeoutRetries := 0
	if args.AllowFlakes {
		flakeRetries = cfg.Defaults.FlakeRetries
		timeoutRetries = cfg.Defaults.TimeoutRetries
	}
	return JobSpec{
		Shortname:      fmt.Sprintf("cloud_to_cloud:%s:%s_server:%s:%s", lang.Name, serverName, testCase, transportSec),
		Cmdline:        cmdline,
		Cwd:            cwd,
		Environ:        environ,
		TimeoutSeconds: cfg.Defaults.TestTimeoutSeconds,
		FlakeRetries:   flakeRetries,
		TimeoutRetries: timeoutRetries,
		ContainerName:  containerName,
	}
}

func buildServerJobSpec(cfg *InteropConfig, lang *LanguageConfig, dockerImage, transportSec string, manualLog *[]string) JobSpec {
	defaultPort := cfg.Defaults.DefaultServerPort
	containerName := fmt.Sprintf("interop_server_%s_%s", lang.Safename, newUUID())
	serverArgs := []string{fmt.Sprintf("--port=%d", defaultPort)}
	secSpec := cfg.TransportSecurity[transportSec]
	serverArgs = append(serverArgs, secSpec.ServerFlags...)

	rendered, _ := lang.ServerCmd.Render(serverArgs)
	cmdline := []string{"bash", "-c", strings.Join(rendered, " ")}
	environ := make(map[string]string)
	for k, v := range lang.GlobalEnv {
		environ[k] = v
	}
	dockerArgs := []string{"--name=" + containerName}
	sdc := lang.ServerDockerConfig
	if sdc != nil && sdc.PortCount > 1 {
		for i := 0; i < sdc.PortCount; i++ {
			dockerArgs = append(dockerArgs, "-p", strconv.Itoa(defaultPort+i))
		}
		if sdc.Healthcheck != nil {
			hc := sdc.Healthcheck
			dockerArgs = append(dockerArgs,
				"--health-cmd="+strings.ReplaceAll(hc.CmdTemplate, "{port}", strconv.Itoa(defaultPort)),
				"--health-interval="+hc.Interval,
				fmt.Sprintf("--health-retries=%d", hc.Retries),
				"--health-timeout="+hc.Timeout,
			)
		}
	} else {
		dockerArgs = append(dockerArgs, "-p", strconv.Itoa(defaultPort))
	}
	dockerCmdline := dockerRunCmdline(cmdline, dockerImage, dockerArgs, lang.ServerCwd, environ, cfg.Defaults.DockerWorkdirRoot)
	if manualLog != nil {
		if len(*manualLog) == 0 {
			*manualLog = append(*manualLog, fmt.Sprintf(`echo "Testing ${docker_image:=%s}"`, dockerImage))
		}
		*manualLog = append(*manualLog, manualCmdline(dockerCmdline, dockerImage))
	}
	return JobSpec{
		Shortname:      "interop_server_" + lang.Name,
		Cmdline:        dockerCmdline,
		Environ:        environ,
		TimeoutSeconds: cfg.Defaults.ServerTimeoutSeconds,
		ContainerName:  containerName,
	}
}

func planImageBuilds(cfg *InteropConfig, languages []*LanguageConfig, servers []string, h2Clients []*LanguageConfig, h2Interop, h2ServerInterop bool) ([]JobSpec, map[string]string) {
	var requested []*LanguageConfig
	seen := make(map[string]bool)
	for _, l := range languages {
		if !seen[l.Name] {
			requested = append(requested, l)
			seen[l.Name] = true
		}
	}
	for _, s := range servers {
		if lcfg, ok := cfg.Languages[s]; ok && !seen[s] {
			requested = append(requested, lcfg)
			seen[s] = true
		}
	}
	for _, hc := range h2Clients {
		if !seen[hc.Name] {
			requested = append(requested, hc)
			seen[hc.Name] = true
		}
	}
	if h2Interop {
		requested = append(requested, cfg.HTTP2Target)
	}
	if h2ServerInterop {
		requested = append(requested, cfg.HTTP2ServerTarget)
	}
	sort.Slice(requested, func(i, j int) bool {
		if requested[i].Safename != requested[j].Safename {
			return requested[i].Safename < requested[j].Safename
		}
		ci := 1
		if requested[i].Name == requested[i].Safename {
			ci = 0
		}
		cj := 1
		if requested[j].Name == requested[j].Safename {
			cj = 0
		}
		if ci != cj {
			return ci < cj
		}
		return requested[i].Name < requested[j].Name
	})

	var buildJobs []JobSpec
	dockerImages := make(map[string]string)
	safenameToTag := make(map[string]string)

	for _, target := range requested {
		if !target.DockerSupported {
			continue
		}
		if tag, ok := safenameToTag[target.Safename]; ok {
			dockerImages[target.Name] = tag
			continue
		}
		canonical := target
		if c, ok := cfg.Languages[target.Safename]; ok && target.Name != target.Safename {
			canonical = c
		}
		tag := fmt.Sprintf("grpc_interop_%s:%s", canonical.Safename, newUUID())
		job := JobSpec{
			Shortname: "build_docker_" + canonical.Name,
			Cmdline:   []string{cfg.Defaults.LegacyImageBuilderScript},
			Environ: map[string]string{
				"INTEROP_IMAGE": tag,
				"BASE_NAME":     "grpc_interop_" + canonical.Safename,
			},
			TimeoutSeconds: cfg.Defaults.BuildImageTimeoutSeconds,
			Tag:            tag,
		}
		safenameToTag[target.Safename] = tag
		dockerImages[target.Name] = tag
		dockerImages[canonical.Name] = tag
		buildJobs = append(buildJobs, job)
	}
	return buildJobs, dockerImages
}

func buildPlan(cfg *InteropConfig, args *CLIArgs) PlanJSON {
	languages, servers, h2Clients := resolveLanguagesAndServers(cfg, args)
	var buildJobs []JobSpec
	dockerImages := make(map[string]string)
	if args.UseDocker {
		buildJobs, dockerImages = planImageBuilds(cfg, languages, servers, h2Clients, args.HTTP2Interop, args.HTTP2ServerInterop)
	}

	var srvManual, cliManual *[]string
	if args.ManualRun {
		s := []string{}
		c := []string{}
		srvManual = &s
		cliManual = &c
	}

	serverSpecs := make(map[string]JobSpec)
	type addrPair struct {
		Name string
		Host string
		Port string
	}
	var serverAddresses []addrPair
	addrMap := make(map[string]int)
	setAddr := func(name, host, port string) {
		if idx, ok := addrMap[name]; ok {
			serverAddresses[idx] = addrPair{name, host, port}
		} else {
			addrMap[name] = len(serverAddresses)
			serverAddresses = append(serverAddresses, addrPair{name, host, port})
		}
	}

	defaultPortStr := strconv.Itoa(cfg.Defaults.DefaultServerPort)
	for _, s := range servers {
		serverSpecs[s] = buildServerJobSpec(cfg, cfg.Languages[s], dockerImages[s], args.TransportSecurity, srvManual)
		if args.ManualRun {
			setAddr(s, "localhost", "${SERVER_PORT}")
		} else {
			setAddr(s, "localhost", defaultPortStr)
		}
	}
	if args.HTTP2ServerInterop {
		h2Name := cfg.HTTP2ServerTarget.Name
		serverSpecs[h2Name] = buildServerJobSpec(cfg, cfg.HTTP2ServerTarget, dockerImages[h2Name], "tls", srvManual)
		if args.ManualRun {
			setAddr(h2Name, "localhost", "${SERVER_PORT}")
		}
	}

	var jobs []JobSpec
	var skipped []SkippedRecord

	if args.CloudToProd {
		for _, nick := range args.ProdServers {
			for _, lang := range languages {
				for _, tc := range cfg.TestCases.Standard {
					if containsStr(lang.ExpandedUnimplClient, tc.Name) {
						skipped = append(skipped, SkippedRecord{"cloud_to_prod", lang.Name, nick, tc.Name, fmt.Sprintf("unimplemented by client '%s'", lang.Name)})
						continue
					}
					if !tc.CloudToProdEligible {
						skipped = append(skipped, SkippedRecord{"cloud_to_prod", lang.Name, nick, tc.Name, "not cloud_to_prod_eligible"})
						continue
					}
					for _, credType := range args.CustomCredentialsType {
						credOpt := cfg.CustomCredentialsTypes[credType]
						if !isAllowed(credOpt.SupportedClients, lang.Name) {
							skipped = append(skipped, SkippedRecord{"cloud_to_prod", lang.Name, nick, tc.Name + ":" + credType, fmt.Sprintf("client '%s' does not support custom_credentials_type '%s'", lang.Name, credType)})
							continue
						}
						jobs = append(jobs, buildCloudToProdJobSpec(cfg, lang, tc.Name, nick, cfg.ProdServers[nick], args, dockerImages[lang.Name], false, credType, cliManual))
					}
				}
			}
			if args.HTTP2Interop {
				h2c := cfg.HTTP2Target
				for _, tc := range cfg.TestCases.HTTP2Client {
					jobs = append(jobs, buildCloudToProdJobSpec(cfg, h2c, tc.Name, nick, cfg.ProdServers[nick], args, dockerImages[h2c.Name], false, args.TransportSecurity, cliManual))
				}
			}
		}
	}

	if args.CloudToProdAuth {
		for _, nick := range args.ProdServers {
			for _, lang := range languages {
				for _, ac := range cfg.TestCases.Auth {
					if args.SkipComputeEngineCreds && ac.NeedsComputeEngineCreds(lang.Name) {
						skipped = append(skipped, SkippedRecord{"cloud_to_prod_auth", lang.Name, nick, ac.Name, "skipped due to --skip_compute_engine_creds"})
						continue
					}
					if containsStr(lang.ExpandedUnimplClient, ac.Name) {
						skipped = append(skipped, SkippedRecord{"cloud_to_prod_auth", lang.Name, nick, ac.Name, fmt.Sprintf("unimplemented by client '%s'", lang.Name)})
						continue
					}
					if !containsStr(args.CustomCredentialsType, ac.TransportSecurity) {
						skipped = append(skipped, SkippedRecord{"cloud_to_prod_auth", lang.Name, nick, ac.Name, fmt.Sprintf("transport_security '%s' not in --custom_credentials_type", ac.TransportSecurity)})
						continue
					}
					jobs = append(jobs, buildCloudToProdJobSpec(cfg, lang, ac.Name, nick, cfg.ProdServers[nick], args, dockerImages[lang.Name], true, ac.TransportSecurity, cliManual))
				}
			}
		}
	}

	for _, ov := range args.OverrideServers {
		lastColon := strings.LastIndexByte(ov[1], ':')
		setAddr(ov[0], ov[1][:lastColon], ov[1][lastColon+1:])
	}

	skipC2CHTTP2 := cfg.Suites["http2_client"].SkipCloudToCloudSrv
	for _, sa := range serverAddresses {
		srvLang := cfg.Languages[sa.Name]
		if srvLang == nil && sa.Name == cfg.HTTP2ServerTarget.Name {
			srvLang = cfg.HTTP2ServerTarget
		}
		var skipSrv []string
		if srvLang != nil {
			skipSrv = srvLang.ExpandedUnimplServer
		}
		for _, lang := range languages {
			for _, tc := range cfg.TestCases.Standard {
				if containsStr(lang.ExpandedUnimplClient, tc.Name) {
					skipped = append(skipped, SkippedRecord{"cloud_to_cloud", lang.Name, sa.Name, tc.Name, fmt.Sprintf("unimplemented by client '%s'", lang.Name)})
					continue
				}
				if containsStr(skipSrv, tc.Name) {
					skipped = append(skipped, SkippedRecord{"cloud_to_cloud", lang.Name, sa.Name, tc.Name, fmt.Sprintf("unimplemented by server '%s'", sa.Name)})
					continue
				}
				jobs = append(jobs, buildCloudToCloudJobSpec(cfg, lang, tc.Name, sa.Name, sa.Host, sa.Port, args, dockerImages[lang.Name], args.TransportSecurity, cliManual))
			}
		}
		if args.HTTP2Interop {
			h2c := cfg.HTTP2Target
			for _, tc := range cfg.TestCases.HTTP2Client {
				if containsStr(skipC2CHTTP2, sa.Name) {
					skipped = append(skipped, SkippedRecord{"http2_client", h2c.Name, sa.Name, tc.Name, fmt.Sprintf("server '%s' skipped for http2_client suite", sa.Name)})
					continue
				}
				jobs = append(jobs, buildCloudToCloudJobSpec(cfg, h2c, tc.Name, sa.Name, sa.Host, sa.Port, args, dockerImages[h2c.Name], args.TransportSecurity, cliManual))
			}
		}
	}

	if args.HTTP2ServerInterop {
		h2SrvName := cfg.HTTP2ServerTarget.Name
		var sortedH2 []string
		for _, c := range cfg.TestCases.HTTP2Server {
			sortedH2 = append(sortedH2, c.Name)
		}
		sort.Strings(sortedH2)
		indexOf := func(name string) int {
			for idx, s := range sortedH2 {
				if s == name {
					return idx
				}
			}
			return 0
		}
		for _, lang := range h2Clients {
			for _, c := range cfg.TestCases.HTTP2Server {
				if _, usesGRPC := cfg.HTTP2ServerGRPCMap[c.Name]; usesGRPC {
					continue
				}
				port := strconv.Itoa(cfg.Defaults.DefaultServerPort + indexOf(c.Name))
				jobs = append(jobs, buildCloudToCloudJobSpec(cfg, lang, c.Name, h2SrvName, "localhost", port, args, dockerImages[lang.Name], args.TransportSecurity, cliManual))
			}
		}
		for _, lang := range languages {
			for _, tcName := range cfg.HTTP2ServerGRPCCases {
				if containsStr(lang.ExpandedUnimplClient, tcName) {
					skipped = append(skipped, SkippedRecord{"http2_server", lang.Name, h2SrvName, tcName, fmt.Sprintf("unimplemented by client '%s'", lang.Name)})
					continue
				}
				port := strconv.Itoa(cfg.Defaults.DefaultServerPort + indexOf(tcName))
				secOverride := cfg.HTTP2ServerMap[tcName].TransportSecurityOverride
				if secOverride == "" {
					secOverride = "insecure"
				}
				jobs = append(jobs, buildCloudToCloudJobSpec(cfg, lang, tcName, h2SrvName, "localhost", port, args, dockerImages[lang.Name], secOverride, cliManual))
			}
		}
	}

	var langNames, h2Names []string
	for _, l := range languages {
		langNames = append(langNames, l.Name)
	}
	for _, l := range h2Clients {
		h2Names = append(h2Names, l.Name)
	}
	if langNames == nil {
		langNames = []string{}
	}
	if servers == nil {
		servers = []string{}
	}
	if h2Names == nil {
		h2Names = []string{}
	}
	if buildJobs == nil {
		buildJobs = []JobSpec{}
	}
	if jobs == nil {
		jobs = []JobSpec{}
	}
	if skipped == nil {
		skipped = []SkippedRecord{}
	}

	return PlanJSON{
		Languages:   langNames,
		Servers:     servers,
		HTTP2Client: h2Names,
		BuildJobs:   buildJobs,
		ServerSpecs: serverSpecs,
		ClientJobs:  jobs,
		Skipped:     skipped,
	}
}

type junitTestSuites struct {
	XMLName xml.Name       `xml:"testsuites"`
	Suites  []junitSuite   `xml:"testsuite"`
}

type junitSuite struct {
	Name      string          `xml:"name,attr"`
	Tests     int             `xml:"tests,attr"`
	Failures  int             `xml:"failures,attr"`
	TestCases []junitTestCase `xml:"testcase"`
}

type junitTestCase struct {
	Name    string        `xml:"name,attr"`
	Time    string        `xml:"time,attr"`
	Failure *junitFailure `xml:"failure,omitempty"`
}

type junitFailure struct {
	Message string `xml:"message,attr"`
	Content string `xml:",chardata"`
}

func runJobsParallel(jobs []JobSpec, maxJobs int) (int, []junitTestCase) {
	if maxJobs < 1 {
		maxJobs = 1
	}
	sem := make(chan struct{}, maxJobs)
	var wg sync.WaitGroup
	var mu sync.Mutex
	failures := 0
	results := make([]junitTestCase, len(jobs))

	for idx, job := range jobs {
		wg.Add(1)
		go func(i int, j JobSpec) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			start := time.Now()
			attempts := 1 + j.FlakeRetries
			var lastErr error
			var lastOut []byte
			for attempt := 0; attempt < attempts; attempt++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(j.TimeoutSeconds)*time.Second)
				cmd := exec.CommandContext(ctx, j.Cmdline[0], j.Cmdline[1:]...)
				if j.Cwd != nil && *j.Cwd != "" {
					cmd.Dir = *j.Cwd
				}
				cmd.Env = os.Environ()
				for k, v := range j.Environ {
					cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
				}
				out, err := cmd.CombinedOutput()
				cancel()
				if err == nil {
					lastErr = nil
					break
				}
				lastErr = err
				lastOut = out
				if j.ContainerName != "" {
					_ = exec.Command("docker", "rm", "-f", j.ContainerName).Run()
				}
			}
			elapsed := time.Since(start).Seconds()
			tc := junitTestCase{
				Name: j.Shortname,
				Time: fmt.Sprintf("%.3f", elapsed),
			}
			if lastErr != nil {
				tc.Failure = &junitFailure{
					Message: "Failure",
					Content: string(lastOut),
				}
			}
			mu.Lock()
			if lastErr != nil {
				failures++
			}
			results[i] = tc
			mu.Unlock()
		}(idx, job)
	}
	wg.Wait()
	return failures, results
}

func main() {
	exePath, err := os.Executable()
	specPath := "tools/interop/interop.json"
	if err == nil {
		candidate := filepath.Join(filepath.Dir(exePath), "interop.json")
		if _, statErr := os.Stat(candidate); statErr == nil {
			specPath = candidate
		}
	}
	if envPath := os.Getenv("GRPC_INTEROP_SPEC"); envPath != "" {
		specPath = envPath
	}
	cfg, err := loadConfig(specPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load %s: %v\n", specPath, err)
		os.Exit(1)
	}

	args, err := parseCLIArgs(os.Args[1:], cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(2)
	}

	if args.MatrixTestcaseProfile != "" {
		prof, ok := cfg.Matrix.TestcaseProfiles[args.MatrixTestcaseProfile]
		if !ok {
			fmt.Fprintf(os.Stderr, "Unknown profile: %s\n", args.MatrixTestcaseProfile)
			os.Exit(1)
		}
		lines := prof.RenderCommands(cfg.Matrix.Servers, cfg.Matrix.TestCases)
		for _, l := range lines {
			fmt.Println(l)
		}
		return
	}

	if args.ListLanguages {
		fmt.Printf("%-15s %-14s %-18s %-7s %-10s %-7s %-10s %s\n",
			"LANGUAGE", "SAFENAME", "REPO", "SERVER", "HTTP2_CLI", "DOCKER", "AUTH_MODE", "CASES(C/S)")
		for _, name := range cfg.LanguageOrder {
			l := cfg.Languages[name]
			fmt.Printf("%-15s %-14s %-18s %-7v %-10v %-7v %-10s %d/%d\n",
				name, l.Safename, l.Repo, l.SupportsServer, l.HTTP2ClientCmd != nil, l.DockerSupported,
				l.AuthCredentialsMode, len(l.ClientTestCases), len(l.ServerTestCases))
		}
		return
	}

	if args.ListTests {
		fmt.Printf("Standard (%d), Auth (%d), HTTP2Client (%d), HTTP2Server (%d)\n",
			len(cfg.TestCases.Standard), len(cfg.TestCases.Auth), len(cfg.TestCases.HTTP2Client), len(cfg.TestCases.HTTP2Server))
		return
	}

	plan := buildPlan(cfg, args)

	if args.DumpPlanJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(plan)
		return
	}

	if args.DryRun || args.ExplainMatrix != "" {
		fmt.Printf("=== Execution Plan (Included=%d, Skipped=%d) ===\n", len(plan.ClientJobs), len(plan.Skipped))
		for _, b := range plan.BuildJobs {
			fmt.Printf("  [BUILD] %s -> %s\n", b.Shortname, b.Tag)
		}
		for _, j := range plan.ClientJobs {
			fmt.Printf("  [TEST] %s : %s\n", j.Shortname, strings.Join(j.Cmdline, " "))
		}
		return
	}

	if args.ManualRun && !args.UseDocker {
		fmt.Println("--manual_run is only supported with --use_docker option enabled.")
		os.Exit(1)
	}
	if !args.UseDocker && (len(plan.Servers) > 0 || len(args.Servers) > 0) {
		fmt.Println("Running interop servers is only supported with --use_docker option enabled.")
		os.Exit(1)
	}
	if len(plan.ClientJobs) == 0 {
		fmt.Println("No jobs to run.")
		os.Exit(1)
	}

	failures, _ := runJobsParallel(plan.ClientJobs, args.Jobs)
	if failures > 0 {
		os.Exit(1)
	}
}
