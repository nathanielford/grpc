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

//! Self-contained gRPC Interop Test Harness (`interop_runner`).
//!
//! Uses only the Rust standard library (`std`). Reads all language, test case,
//! authentication, Docker build, server healthcheck, and release matrix rules
//! from `tools/interop/config.json` with zero hardcoded language names or
//! hardcoded test case skip lists.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::env;
use std::fmt::Write as FmtWrite;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

// ============================================================================
// Minimal Recursive-Descent JSON Parser & Serializer (Preserving Key Order)
// ============================================================================

#[derive(Clone, Debug, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    Number(f64),
    Str(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

impl Json {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Json::Str(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Json::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Json::Number(n) => Some(*n as i64),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&Vec<Json>> {
        match self {
            Json::Array(a) => Some(a),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<&Vec<(String, Json)>> {
        match self {
            Json::Object(o) => Some(o),
            _ => None,
        }
    }

    pub fn get(&self, key: &str) -> Option<&Json> {
        match self {
            Json::Object(entries) => {
                for (k, v) in entries {
                    if k == key {
                        return Some(v);
                    }
                }
                None
            }
            _ => None,
        }
    }

    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(|v| v.as_str())
    }

    pub fn get_bool(&self, key: &str, default: bool) -> bool {
        self.get(key).and_then(|v| v.as_bool()).unwrap_or(default)
    }

    pub fn get_i64(&self, key: &str, default: i64) -> i64 {
        self.get(key).and_then(|v| v.as_i64()).unwrap_or(default)
    }

    pub fn get_string_vec(&self, key: &str) -> Vec<String> {
        self.get(key)
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|item| item.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn get_string_map(&self, key: &str) -> Vec<(String, String)> {
        self.get(key)
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn to_json_string(&self) -> String {
        let mut out = String::new();
        self.write_json(&mut out);
        out
    }

    fn write_json(&self, out: &mut String) {
        match self {
            Json::Null => out.push_str("null"),
            Json::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Json::Number(n) => {
                if n.fract() == 0.0 && n.abs() < 1e15 {
                    let _ = write!(out, "{}", *n as i64);
                } else {
                    let _ = write!(out, "{}", n);
                }
            }
            Json::Str(s) => {
                out.push('"');
                for ch in s.chars() {
                    match ch {
                        '"' => out.push_str("\\\""),
                        '\\' => out.push_str("\\\\"),
                        '\n' => out.push_str("\\n"),
                        '\r' => out.push_str("\\r"),
                        '\t' => out.push_str("\\t"),
                        c if (c as u32) < 0x20 => {
                            let _ = write!(out, "\\u{:04x}", c as u32);
                        }
                        c => out.push(c),
                    }
                }
                out.push('"');
            }
            Json::Array(items) => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    item.write_json(out);
                }
                out.push(']');
            }
            Json::Object(entries) => {
                out.push('{');
                for (i, (k, v)) in entries.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    Json::Str(k.clone()).write_json(out);
                    out.push(':');
                    v.write_json(out);
                }
                out.push('}');
            }
        }
    }
}

struct JsonParser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> JsonParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            bytes: input.as_bytes(),
            pos: 0,
        }
    }

    fn skip_ws(&mut self) {
        while self.pos < self.bytes.len() {
            match self.bytes[self.pos] {
                b' ' | b'\t' | b'\n' | b'\r' => self.pos += 1,
                _ => break,
            }
        }
    }

    fn peek(&mut self) -> Option<u8> {
        self.skip_ws();
        self.bytes.get(self.pos).copied()
    }

    fn parse_value(&mut self) -> Result<Json, String> {
        match self.peek() {
            Some(b'n') => self.parse_null(),
            Some(b't') | Some(b'f') => self.parse_bool(),
            Some(b'"') => self.parse_string().map(Json::Str),
            Some(b'[') => self.parse_array(),
            Some(b'{') => self.parse_object(),
            Some(c) if c == b'-' || c.is_ascii_digit() => self.parse_number(),
            Some(c) => Err(format!(
                "Unexpected character '{}' at byte {}",
                c as char, self.pos
            )),
            None => Err("Unexpected EOF while parsing JSON".to_string()),
        }
    }

    fn parse_null(&mut self) -> Result<Json, String> {
        if self.bytes[self.pos..].starts_with(b"null") {
            self.pos += 4;
            Ok(Json::Null)
        } else {
            Err(format!("Invalid token at byte {}", self.pos))
        }
    }

    fn parse_bool(&mut self) -> Result<Json, String> {
        if self.bytes[self.pos..].starts_with(b"true") {
            self.pos += 4;
            Ok(Json::Bool(true))
        } else if self.bytes[self.pos..].starts_with(b"false") {
            self.pos += 5;
            Ok(Json::Bool(false))
        } else {
            Err(format!("Invalid boolean token at byte {}", self.pos))
        }
    }

    fn parse_number(&mut self) -> Result<Json, String> {
        let start = self.pos;
        if self.bytes.get(self.pos) == Some(&b'-') {
            self.pos += 1;
        }
        while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
        if self.bytes.get(self.pos) == Some(&b'.') {
            self.pos += 1;
            while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
        }
        if matches!(self.bytes.get(self.pos), Some(b'e') | Some(b'E')) {
            self.pos += 1;
            if matches!(self.bytes.get(self.pos), Some(b'+') | Some(b'-')) {
                self.pos += 1;
            }
            while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
        }
        let s = std::str::from_utf8(&self.bytes[start..self.pos])
            .map_err(|e| format!("Invalid UTF-8 in number: {}", e))?;
        let val = s
            .parse::<f64>()
            .map_err(|e| format!("Invalid number '{}': {}", s, e))?;
        Ok(Json::Number(val))
    }

    fn parse_string(&mut self) -> Result<String, String> {
        self.skip_ws();
        if self.bytes.get(self.pos) != Some(&b'"') {
            return Err(format!("Expected '\"' at byte {}", self.pos));
        }
        self.pos += 1;
        let mut out = String::new();
        while self.pos < self.bytes.len() {
            let b = self.bytes[self.pos];
            if b == b'"' {
                self.pos += 1;
                return Ok(out);
            } else if b == b'\\' {
                self.pos += 1;
                if self.pos >= self.bytes.len() {
                    return Err("Unexpected EOF in string escape".to_string());
                }
                let esc = self.bytes[self.pos];
                self.pos += 1;
                match esc {
                    b'"' => out.push('"'),
                    b'\\' => out.push('\\'),
                    b'/' => out.push('/'),
                    b'b' => out.push('\u{0008}'),
                    b'f' => out.push('\u{000c}'),
                    b'n' => out.push('\n'),
                    b'r' => out.push('\r'),
                    b't' => out.push('\t'),
                    b'u' => {
                        if self.pos + 4 > self.bytes.len() {
                            return Err("Unexpected EOF in \\u escape".to_string());
                        }
                        let hex = std::str::from_utf8(&self.bytes[self.pos..self.pos + 4])
                            .map_err(|_| "Invalid hex in \\u escape".to_string())?;
                        let code = u32::from_str_radix(hex, 16)
                            .map_err(|_| format!("Invalid hex '{}' in \\u escape", hex))?;
                        self.pos += 4;
                        if let Some(ch) = char::from_u32(code) {
                            out.push(ch);
                        }
                    }
                    other => out.push(other as char),
                }
            } else {
                let ch_str = std::str::from_utf8(&self.bytes[self.pos..])
                    .map_err(|e| format!("Invalid UTF-8 at {}: {}", self.pos, e))?;
                let first_char = ch_str.chars().next().unwrap();
                out.push(first_char);
                self.pos += first_char.len_utf8();
            }
        }
        Err("Unterminated JSON string".to_string())
    }

    fn parse_array(&mut self) -> Result<Json, String> {
        self.skip_ws();
        self.pos += 1; // '['
        let mut items = Vec::new();
        if self.peek() == Some(b']') {
            self.pos += 1;
            return Ok(Json::Array(items));
        }
        loop {
            items.push(self.parse_value()?);
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b']') => {
                    self.pos += 1;
                    break;
                }
                other => {
                    return Err(format!(
                        "Expected ',' or ']' in array at byte {}, got {:?}",
                        self.pos,
                        other.map(|b| b as char)
                    ))
                }
            }
        }
        Ok(Json::Array(items))
    }

    fn parse_object(&mut self) -> Result<Json, String> {
        self.skip_ws();
        self.pos += 1; // '{'
        let mut entries = Vec::new();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(Json::Object(entries));
        }
        loop {
            let key = self.parse_string()?;
            if self.peek() != Some(b':') {
                return Err(format!("Expected ':' after object key at byte {}", self.pos));
            }
            self.pos += 1;
            let val = self.parse_value()?;
            entries.push((key, val));
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b'}') => {
                    self.pos += 1;
                    break;
                }
                other => {
                    return Err(format!(
                        "Expected ',' or '}}' in object at byte {}, got {:?}",
                        self.pos,
                        other.map(|b| b as char)
                    ))
                }
            }
        }
        Ok(Json::Object(entries))
    }
}

pub fn parse_json(input: &str) -> Result<Json, String> {
    let mut parser = JsonParser::new(input);
    let val = parser.parse_value()?;
    parser.skip_ws();
    if parser.pos != parser.bytes.len() {
        return Err(format!("Trailing characters at byte {}", parser.pos));
    }
    Ok(val)
}

// ============================================================================
// Configuration Validation & Data Structures
// ============================================================================

pub fn validate_config(root: &Json) -> Result<(), String> {
    let required_keys = [
        "schema_version",
        "defaults",
        "prod_servers",
        "transport_security",
        "custom_credentials_types",
        "test_case_groups",
        "suites",
        "test_cases",
        "languages",
        "matrix",
    ];
    for k in &required_keys {
        if root.get(k).is_none() {
            return Err(format!("Missing required top-level key '{}'", k));
        }
    }
    let ver = root.get_str("schema_version").unwrap_or("");
    if ver != "1" && ver != "1.0" {
        return Err(format!("Unsupported schema_version: '{}'", ver));
    }
    let tc_sec = root.get("test_cases").unwrap();
    for cat in &["standard", "auth", "http2_client", "http2_server"] {
        match tc_sec.get(cat).and_then(|v| v.as_array()) {
            Some(arr) if !arr.is_empty() => {}
            _ => return Err(format!("test_cases.{} must be a non-empty list", cat)),
        }
    }
    let mut known_cases = BTreeSet::new();
    for cat in &["standard", "auth", "http2_client", "http2_server"] {
        for item in tc_sec.get(cat).unwrap().as_array().unwrap() {
            if let Some(name) = item.get_str("name") {
                known_cases.insert(name.to_string());
            } else {
                return Err(format!("Entry in test_cases.{} missing 'name'", cat));
            }
        }
    }

    let groups_obj = root
        .get("test_case_groups")
        .and_then(|v| v.as_object())
        .ok_or_else(|| "test_case_groups must be an object".to_string())?;
    let mut group_names: BTreeSet<String> = [
        "standard_test_cases",
        "standard_test_cases_except_first",
        "auth_test_cases",
        "http2_client_test_cases",
        "http2_server_test_cases",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();

    for (gname, gitems) in groups_obj {
        group_names.insert(gname.clone());
        let arr = gitems
            .as_array()
            .ok_or_else(|| format!("test_case_groups.{} must be a list", gname))?;
        for item in arr {
            let cname = item.as_str().unwrap_or("");
            if !known_cases.contains(cname) {
                return Err(format!(
                    "Unknown test case '{}' in test_case_groups.{}",
                    cname, gname
                ));
            }
        }
    }

    let langs_obj = root
        .get("languages")
        .and_then(|v| v.as_object())
        .ok_or_else(|| "languages must be a non-empty object".to_string())?;
    if langs_obj.is_empty() {
        return Err("languages must not be empty".to_string());
    }
    let valid_modes = ["append", "joined_flag", "ignore_args", "extract_port_template"];
    for (lname, ldata) in langs_obj {
        if ldata.get_str("safename").unwrap_or("").is_empty() {
            return Err(format!("Language '{}' missing 'safename'", lname));
        }
        let auth_mode = ldata.get_str("auth_credentials_mode").unwrap_or("");
        if auth_mode != "env" && auth_mode != "flag" {
            return Err(format!(
                "Language '{}' has invalid auth_credentials_mode '{}'",
                lname, auth_mode
            ));
        }
        if ldata.get_bool("supports_server", false) && ldata.get("server_cmd").is_none() {
            return Err(format!(
                "Language '{}' has supports_server=true but no server_cmd",
                lname
            ));
        }
        for cmd_key in &["client_cmd", "server_cmd", "http2_client_cmd"] {
            if let Some(cmd_obj) = ldata.get(cmd_key) {
                if *cmd_obj != Json::Null {
                    let mode = cmd_obj.get_str("args_mode").unwrap_or("append");
                    if !valid_modes.contains(&mode) {
                        return Err(format!(
                            "Language '{}.{}' has invalid args_mode '{}'",
                            lname, cmd_key, mode
                        ));
                    }
                }
            }
        }
        for list_key in &[
            "unimplemented_client_cases",
            "unimplemented_server_cases",
            "server_unimplemented_client_cases",
        ] {
            for r in ldata.get_string_vec(list_key) {
                if let Some(gref) = r.strip_prefix('@') {
                    if !group_names.contains(gref) {
                        return Err(format!(
                            "Language '{}.{}' references unknown group '{}'",
                            lname, list_key, r
                        ));
                    }
                } else if !known_cases.contains(&r) {
                    return Err(format!(
                        "Language '{}.{}' references unknown test case '{}'",
                        lname, list_key, r
                    ));
                }
            }
        }
    }

    let matrix = root.get("matrix").unwrap();
    let runtimes = matrix
        .get("runtimes")
        .and_then(|v| v.as_object())
        .ok_or_else(|| "matrix.runtimes must be an object".to_string())?;
    let runtime_names: BTreeSet<String> = runtimes.iter().map(|(k, _)| k.clone()).collect();
    let profiles = matrix
        .get("testcase_profiles")
        .and_then(|v| v.as_object())
        .ok_or_else(|| "matrix.testcase_profiles must be an object".to_string())?;
    let profile_names: BTreeSet<String> = profiles.iter().map(|(k, _)| k.clone()).collect();

    if let Some(mlangs) = matrix.get("languages").and_then(|v| v.as_object()) {
        for (mlang, mdata) in mlangs {
            for r in mdata.get_string_vec("default_runtimes") {
                if !runtime_names.contains(&r) {
                    return Err(format!(
                        "matrix.languages.{}.default_runtimes references unknown runtime '{}'",
                        mlang, r
                    ));
                }
            }
            if let Some(rels) = mdata.get("releases").and_then(|v| v.as_object()) {
                for (rtag, rinfo) in rels {
                    for r in rinfo.get_string_vec("runtimes") {
                        if !runtime_names.contains(&r) {
                            return Err(format!(
                                "matrix.languages.{}.releases.{} references unknown runtime '{}'",
                                mlang, rtag, r
                            ));
                        }
                    }
                    if let Some(tcf) = rinfo.get_str("testcases_file") {
                        if !profile_names.contains(tcf) {
                            return Err(format!(
                                "matrix.languages.{}.releases.{} references unknown testcase_profile '{}'",
                                mlang, rtag, tcf
                            ));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

// ============================================================================
// Command Rendering & Plan Construction (100% Config-Driven)
// ============================================================================

fn render_command(cmd_spec: &Json, args: &[String]) -> Option<Vec<String>> {
    let base_cmd = cmd_spec.get_string_vec("command");
    let mode = cmd_spec.get_str("args_mode").unwrap_or("append");
    match mode {
        "append" => {
            let mut out = base_cmd;
            out.extend_from_slice(args);
            Some(out)
        }
        "joined_flag" => {
            let joined = args.join(" ");
            let tmpl = cmd_spec.get_str("arg_template").unwrap_or("--args=\"{args}\"");
            let formatted = tmpl.replace("{args}", &joined);
            let mut out = base_cmd;
            out.push(formatted);
            Some(out)
        }
        "ignore_args" => Some(base_cmd),
        "extract_port_template" => {
            let tmpl = cmd_spec.get_str("arg_template").unwrap_or("");
            for arg in args {
                if let Some(port_val) = arg.strip_prefix("--server_port=") {
                    if !port_val.is_empty() && port_val.chars().all(|c| c.is_ascii_digit()) {
                        return Some(vec![tmpl.replace("{port}", port_val)]);
                    }
                }
            }
            None
        }
        _ => None,
    }
}

fn expand_cases(root: &Json, refs: &[String]) -> Vec<String> {
    let tc_sec = root.get("test_cases").unwrap();
    let std_names: Vec<String> = tc_sec
        .get("standard")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|c| c.get_str("name").map(|s| s.to_string()))
        .collect();
    let auth_names: Vec<String> = tc_sec
        .get("auth")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|c| c.get_str("name").map(|s| s.to_string()))
        .collect();
    let h2_cli_names: Vec<String> = tc_sec
        .get("http2_client")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|c| c.get_str("name").map(|s| s.to_string()))
        .collect();
    let h2_srv_names: Vec<String> = tc_sec
        .get("http2_server")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|c| c.get_str("name").map(|s| s.to_string()))
        .collect();

    let groups = root.get("test_case_groups").unwrap();
    let mut out = Vec::new();
    for r in refs {
        if let Some(g) = r.strip_prefix('@') {
            if let Some(g_arr) = groups.get(g) {
                for item in g_arr.as_array().unwrap() {
                    if let Some(s) = item.as_str() {
                        out.push(s.to_string());
                    }
                }
            } else {
                match g {
                    "standard_test_cases" => out.extend(std_names.iter().cloned()),
                    "standard_test_cases_except_first" => {
                        out.extend(std_names.iter().skip(1).cloned())
                    }
                    "auth_test_cases" => out.extend(auth_names.iter().cloned()),
                    "http2_client_test_cases" => out.extend(h2_cli_names.iter().cloned()),
                    "http2_server_test_cases" => out.extend(h2_srv_names.iter().cloned()),
                    _ => {}
                }
            }
        } else {
            out.push(r.clone());
        }
    }
    out
}

fn lang_unimplemented_client(root: &Json, ldata: &Json, as_server_target: bool) -> Vec<String> {
    if as_server_target {
        if ldata.get("expanded_server_unimplemented_client_cases").is_some() {
            return ldata.get_string_vec("expanded_server_unimplemented_client_cases");
        }
        if ldata.get("server_unimplemented_client_cases").is_some() {
            return expand_cases(
                root,
                &ldata.get_string_vec("server_unimplemented_client_cases"),
            );
        }
    }
    if ldata.get("expanded_unimplemented_client_cases").is_some() {
        return ldata.get_string_vec("expanded_unimplemented_client_cases");
    }
    expand_cases(root, &ldata.get_string_vec("unimplemented_client_cases"))
}

fn lang_unimplemented_server(root: &Json, ldata: &Json) -> Vec<String> {
    if ldata.get("expanded_unimplemented_server_cases").is_some() {
        return ldata.get_string_vec("expanded_unimplemented_server_cases");
    }
    expand_cases(root, &ldata.get_string_vec("unimplemented_server_cases"))
}

fn find_http2_target_name(root: &Json) -> String {
    if let Some(langs) = root.get("languages").and_then(|v| v.as_object()) {
        for (name, ldata) in langs {
            if ldata.get_bool("is_http2_target", false) {
                return name.clone();
            }
        }
    }
    String::new()
}

pub fn render_matrix_profile(root: &Json, profile_name: &str) -> Result<Vec<String>, String> {
    let matrix = root.get("matrix").ok_or("Missing matrix section")?;
    let profiles = matrix
        .get("testcase_profiles")
        .ok_or("Missing testcase_profiles")?;
    let prof = profiles
        .get(profile_name)
        .ok_or_else(|| format!("Unknown matrix testcase profile '{}'", profile_name))?;
    let servers = matrix.get_string_vec("servers");
    let test_cases = matrix.get_string_vec("test_cases");
    let workdir = prof.get_str("workdir").unwrap_or("/var/local/git/grpc");
    let inner_tmpl = prof.get_str("inner_command_template").unwrap_or("");
    let bash_login = prof.get_bool("bash_login", false);
    let container_names = prof.get_string_vec("container_names");
    let env_pairs = prof.get_string_map("env");
    let env_str = if env_pairs.is_empty() {
        String::new()
    } else {
        let joined: Vec<String> = env_pairs
            .iter()
            .map(|(k, v)| format!("-e {}={}", k, v))
            .collect();
        format!(" {}", joined.join(" "))
    };
    let bash_cmd = if bash_login { "bash -l -c" } else { "bash -c" };
    let mut lines = Vec::new();
    let mut idx = 0;
    for srv in &servers {
        for tc in &test_cases {
            let name_str = if idx < container_names.len() {
                format!(" --name {}", container_names[idx])
            } else {
                String::new()
            };
            let inner = inner_tmpl
                .replace("{server_host}", srv)
                .replace("{test_case}", tc);
            lines.push(format!(
                "docker run -i --rm=true{} -w {} --net=host{} $docker_image {} \"{}\"",
                env_str, workdir, name_str, bash_cmd, inner
            ));
            idx += 1;
        }
    }
    Ok(lines)
}

// ============================================================================
// CLI Arguments & Plan Generator
// ============================================================================

#[derive(Clone, Debug)]
pub struct CliArgs {
    pub config_path: PathBuf,
    pub validate_only: bool,
    pub list_languages: bool,
    pub list_tests: bool,
    pub explain_matrix: Option<String>,
    pub render_profile: Option<String>,
    pub plan_json: bool,
    pub dry_run: bool,
    pub languages: Vec<String>,
    pub jobs: usize,
    pub cloud_to_prod: bool,
    pub cloud_to_prod_auth: bool,
    pub google_default_creds_use_key_file: bool,
    pub prod_servers: Vec<String>,
    pub servers: Vec<String>,
    pub override_servers: Vec<(String, String)>,
    pub service_account_key_file: Option<String>,
    pub default_service_account: Option<String>,
    pub travis: bool,
    pub verbose: bool,
    pub use_docker: bool,
    pub allow_flakes: bool,
    pub manual_run: bool,
    pub http2_interop: bool,
    pub http2_server_interop: bool,
    pub transport_security: String,
    pub custom_credentials_type: Vec<String>,
    pub skip_compute_engine_creds: bool,
    pub bq_result_table: String,
}

impl CliArgs {
    fn default_config_path() -> PathBuf {
        if let Ok(exe) = env::current_exe() {
            if let Some(dir) = exe.parent() {
                let candidate = dir.join("config.json");
                if candidate.exists() {
                    return candidate;
                }
            }
        }
        PathBuf::from("tools/interop/config.json")
    }

    pub fn parse_from_env() -> Result<Self, String> {
        let raw_args: Vec<String> = env::args().skip(1).collect();
        let mut args = Self {
            config_path: Self::default_config_path(),
            validate_only: false,
            list_languages: false,
            list_tests: false,
            explain_matrix: None,
            render_profile: None,
            plan_json: false,
            dry_run: false,
            languages: vec!["all".to_string()],
            jobs: thread::available_parallelism().map(|n| n.get()).unwrap_or(4),
            cloud_to_prod: false,
            cloud_to_prod_auth: false,
            google_default_creds_use_key_file: false,
            prod_servers: vec!["default".to_string()],
            servers: Vec::new(),
            override_servers: Vec::new(),
            service_account_key_file: None,
            default_service_account: None,
            travis: false,
            verbose: false,
            use_docker: false,
            allow_flakes: false,
            manual_run: false,
            http2_interop: false,
            http2_server_interop: false,
            transport_security: "tls".to_string(),
            custom_credentials_type: Vec::new(),
            skip_compute_engine_creds: false,
            bq_result_table: String::new(),
        };

        let mut seen_lang = false;
        let mut seen_srv = false;
        let mut seen_prod = false;
        let mut seen_creds = false;
        let mut i = 0;

        while i < raw_args.len() {
            let arg = &raw_args[i];
            match arg.as_str() {
                "-h" | "--help" => {
                    print_help();
                    std::process::exit(0);
                }
                "--config" => {
                    i += 1;
                    args.config_path = PathBuf::from(
                        raw_args.get(i).ok_or("--config requires a path")?,
                    );
                }
                "--validate" => args.validate_only = true,
                "--list-languages" => args.list_languages = true,
                "--list-tests" => args.list_tests = true,
                "--plan-json" => args.plan_json = true,
                "--dry-run" => args.dry_run = true,
                "--explain-matrix" => {
                    if i + 1 < raw_args.len() && !raw_args[i + 1].starts_with('-') {
                        i += 1;
                        args.explain_matrix = Some(raw_args[i].clone());
                    } else {
                        args.explain_matrix = Some("all".to_string());
                    }
                }
                "--render-matrix-profile" => {
                    i += 1;
                    args.render_profile = Some(
                        raw_args
                            .get(i)
                            .ok_or("--render-matrix-profile requires a profile name")?
                            .clone(),
                    );
                }
                "-l" | "--language" => {
                    if !seen_lang {
                        seen_lang = true;
                        args.languages.clear();
                    }
                    while i + 1 < raw_args.len() && !raw_args[i + 1].starts_with('-') {
                        i += 1;
                        if !args.languages.contains(&raw_args[i]) {
                            args.languages.push(raw_args[i].clone());
                        }
                    }
                }
                "-s" | "--server" => {
                    if !seen_srv {
                        seen_srv = true;
                        args.servers.clear();
                    }
                    while i + 1 < raw_args.len() && !raw_args[i + 1].starts_with('-') {
                        i += 1;
                        if !args.servers.contains(&raw_args[i]) {
                            args.servers.push(raw_args[i].clone());
                        }
                    }
                }
                "--prod_servers" => {
                    if !seen_prod {
                        seen_prod = true;
                        args.prod_servers.clear();
                    }
                    while i + 1 < raw_args.len() && !raw_args[i + 1].starts_with('-') {
                        i += 1;
                        args.prod_servers.push(raw_args[i].clone());
                    }
                }
                "--custom_credentials_type" => {
                    if !seen_creds {
                        seen_creds = true;
                        args.custom_credentials_type.clear();
                    }
                    while i + 1 < raw_args.len() && !raw_args[i + 1].starts_with('-') {
                        i += 1;
                        args.custom_credentials_type.push(raw_args[i].clone());
                    }
                }
                "--override_server" => {
                    i += 1;
                    let kv = raw_args
                        .get(i)
                        .ok_or("--override_server requires server=HOST:PORT")?;
                    if let Some((k, v)) = kv.split_once('=') {
                        args.override_servers.push((k.to_string(), v.to_string()));
                    } else {
                        return Err(format!("Invalid --override_server '{}'", kv));
                    }
                }
                "-j" | "--jobs" => {
                    i += 1;
                    args.jobs = raw_args
                        .get(i)
                        .ok_or("-j/--jobs requires an integer")?
                        .parse::<usize>()
                        .map_err(|_| "Invalid --jobs integer")?;
                }
                "--cloud_to_prod" => args.cloud_to_prod = true,
                "--cloud_to_prod_auth" => args.cloud_to_prod_auth = true,
                "--google_default_creds_use_key_file" => {
                    args.google_default_creds_use_key_file = true
                }
                "--service_account_key_file" => {
                    i += 1;
                    args.service_account_key_file =
                        Some(raw_args.get(i).ok_or("Missing key file")?.clone());
                }
                "--default_service_account" => {
                    i += 1;
                    args.default_service_account =
                        Some(raw_args.get(i).ok_or("Missing service account")?.clone());
                }
                "-t" | "--travis" => args.travis = true,
                "-v" | "--verbose" => args.verbose = true,
                "--use_docker" => args.use_docker = true,
                "--allow_flakes" => args.allow_flakes = true,
                "--manual_run" => args.manual_run = true,
                "--http2_interop" => args.http2_interop = true,
                "--http2_server_interop" => args.http2_server_interop = true,
                "--transport_security" => {
                    i += 1;
                    args.transport_security = raw_args
                        .get(i)
                        .ok_or("Missing transport_security value")?
                        .clone();
                }
                "--skip_compute_engine_creds" => args.skip_compute_engine_creds = true,
                "--bq_result_table" => {
                    if i + 1 < raw_args.len() && !raw_args[i + 1].starts_with('-') {
                        i += 1;
                        args.bq_result_table = raw_args[i].clone();
                    }
                }
                other => {
                    if let Some(v) = other.strip_prefix("--transport_security=") {
                        args.transport_security = v.to_string();
                    } else if let Some(v) = other.strip_prefix("--service_account_key_file=") {
                        args.service_account_key_file = Some(v.to_string());
                    } else if let Some(v) = other.strip_prefix("--default_service_account=") {
                        args.default_service_account = Some(v.to_string());
                    } else if let Some(v) = other.strip_prefix("--override_server=") {
                        if let Some((k, hp)) = v.split_once('=') {
                            args.override_servers.push((k.to_string(), hp.to_string()));
                        }
                    } else {
                        return Err(format!("Unrecognized argument: {}", other));
                    }
                }
            }
            i += 1;
        }
        Ok(args)
    }
}

fn print_help() {
    println!(
        "Usage: interop_runner [OPTIONS]

Options:
  --config PATH                        Path to config.json (default: tools/interop/config.json)
  --validate                           Validate config.json schema and cross-references
  --list-languages                     Print table of configured languages and capabilities
  --list-tests                         Print configured test suites and test cases
  --explain-matrix [LANG|all]          Explain included and skipped test cases
  --render-matrix-profile PROFILE      Render docker run commands for a matrix profile
  --plan-json                          Emit resolved execution plan as JSON
  --dry-run                            Print execution plan without running containers
  -l, --language LANG [LANG ...]       Client languages to run (or 'all')
  -s, --server SERVER [SERVER ...]     Server languages to run in Docker (or 'all')
  --override_server SERVER=HOST:PORT   Override server address
  --cloud_to_prod                      Run cloud_to_prod tests
  --cloud_to_prod_auth                 Run cloud_to_prod_auth tests
  --prod_servers NICK [NICK ...]       Prod servers to test (default, gateway_v4)
  --google_default_creds_use_key_file  Set GOOGLE_APPLICATION_CREDENTIALS for google_default_credentials
  --service_account_key_file PATH      Service account key file path
  --default_service_account EMAIL      Default GCE service account email
  --transport_security SEC             Transport security (tls, alts, insecure)
  --custom_credentials_type CRED ...   Custom credentials types for cloud_to_prod
  --skip_compute_engine_creds          Skip auth tests requiring GCE credentials
  --http2_interop                      Enable HTTP/2 bad-client interop tests
  --http2_server_interop               Enable HTTP/2 server interop tests
  --use_docker                         Build and run under Docker
  --manual_run                         Generate manual command scripts without running tests
  --allow_flakes                       Allow flake retries
  -j, --jobs N                         Max parallel jobs
  -t, --travis                         CI mode
  -v, --verbose                        Verbose output"
    );
}

pub fn print_languages_table(root: &Json) {
    println!(
        "{:<15} {:<14} {:<18} {:<7} {:<10} {:<7} {:<10} {}",
        "LANGUAGE", "SAFENAME", "REPO", "SERVER", "HTTP2_CLI", "DOCKER", "AUTH_MODE", "CASES(C/S)"
    );
    println!("{}", "-".repeat(98));
    if let Some(langs) = root.get("languages").and_then(|v| v.as_object()) {
        for (name, ldata) in langs {
            let safename = ldata.get_str("safename").unwrap_or("");
            let repo = ldata.get_str("repo").unwrap_or("");
            let srv = ldata.get_bool("supports_server", false);
            let h2 = ldata
                .get("http2_client_cmd")
                .map(|v| *v != Json::Null)
                .unwrap_or(false);
            let docker = ldata.get_bool("docker_supported", true);
            let auth = ldata.get_str("auth_credentials_mode").unwrap_or("flag");
            let cc = ldata.get_string_vec("client_test_cases").len();
            let sc = ldata.get_string_vec("server_test_cases").len();
            println!(
                "{:<15} {:<14} {:<18} {:<7} {:<10} {:<7} {:<10} {}/{}",
                name,
                safename,
                repo,
                if srv { "yes" } else { "no" },
                if h2 { "yes" } else { "no" },
                if docker { "yes" } else { "no" },
                auth,
                cc,
                sc
            );
        }
    }
}

pub fn print_tests_catalog(root: &Json) {
    let tc_sec = root.get("test_cases").unwrap();
    println!("=== Standard Test Cases ===");
    for c in tc_sec.get("standard").unwrap().as_array().unwrap() {
        let name = c.get_str("name").unwrap_or("");
        let c2p = c.get_bool("cloud_to_prod_eligible", false);
        println!(
            "  - {:<34} (cloud_to_prod: {})",
            name,
            if c2p { "yes" } else { "no" }
        );
    }
    println!("\n=== Auth Test Cases ===");
    for c in tc_sec.get("auth").unwrap().as_array().unwrap() {
        let name = c.get_str("name").unwrap_or("");
        let sec = c.get_str("transport_security").unwrap_or("tls");
        let gce = c.get_bool("requires_compute_engine_creds", false);
        println!(
            "  - {:<34} (security: {}, gce_creds: {})",
            name,
            sec,
            if gce { "yes" } else { "no" }
        );
    }
    println!("\n=== HTTP/2 Client Test Cases ===");
    for c in tc_sec.get("http2_client").unwrap().as_array().unwrap() {
        println!("  - {}", c.get_str("name").unwrap_or(""));
    }
    println!("\n=== HTTP/2 Server Test Cases ===");
    for c in tc_sec.get("http2_server").unwrap().as_array().unwrap() {
        let name = c.get_str("name").unwrap_or("");
        let kind = c.get_str("client_kind").unwrap_or("http2_interop");
        println!("  - {:<34} (client_kind: {})", name, kind);
    }
}

pub fn print_explain_matrix(root: &Json, filter: &str, transport_security: &str) {
    let langs_obj = root.get("languages").unwrap().as_object().unwrap();
    let tc_sec = root.get("test_cases").unwrap();
    let std_cases: Vec<String> = tc_sec
        .get("standard")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|c| c.get_str("name").map(|s| s.to_string()))
        .collect();

    let mut clients = Vec::new();
    let mut servers = Vec::new();
    for (name, ldata) in langs_obj {
        if ldata.get_bool("is_http2_target", false) {
            continue;
        }
        if filter == "all" || filter == name {
            clients.push(name.clone());
        }
        if ldata.get_bool("supports_server", false)
            && !ldata.get_string_vec("server_test_cases").is_empty()
        {
            servers.push(name.clone());
        }
    }

    let mut included = 0;
    let mut skipped = 0;
    for s in &servers {
        let s_data = &langs_obj.iter().find(|(k, _)| k == s).unwrap().1;
        let skip_s = lang_unimplemented_server(root, s_data);
        for c in &clients {
            let c_data = &langs_obj.iter().find(|(k, _)| k == c).unwrap().1;
            let skip_c = lang_unimplemented_client(root, c_data, false);
            for tc in &std_cases {
                if skip_c.contains(tc) {
                    skipped += 1;
                    println!(
                        "[SKIPPED]  cloud_to_cloud:{}:{}_server:{}:{} -> unimplemented by client '{}'",
                        c, s, tc, transport_security, c
                    );
                } else if skip_s.contains(tc) {
                    skipped += 1;
                    println!(
                        "[SKIPPED]  cloud_to_cloud:{}:{}_server:{}:{} -> unimplemented by server '{}'",
                        c, s, tc, transport_security, s
                    );
                } else {
                    included += 1;
                    println!(
                        "[INCLUDED] cloud_to_cloud:{}:{}_server:{}:{}",
                        c, s, tc, transport_security
                    );
                }
            }
        }
    }
    println!(
        "\nSummary: {} included, {} skipped (total {})",
        included,
        skipped,
        included + skipped
    );
}

fn pseudo_uuid(seed: usize) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let pid = std::process::id() as u128;
    let x = nanos
        .wrapping_mul(6364136223846793005)
        .wrapping_add(pid << 32)
        .wrapping_add(seed as u128 * 1442695040888963407);
    format!(
        "{:08x}-{:04x}-4{:03x}-a{:03x}-{:012x}",
        (x >> 96) as u32,
        ((x >> 80) & 0xffff) as u16,
        ((x >> 64) & 0x0fff) as u16,
        ((x >> 48) & 0x0fff) as u16,
        (x & 0xffffffffffff) as u64
    )
}

fn is_supported_for_sec(sec_obj: &Json, role_key: &str, lang: &str) -> bool {
    match sec_obj.get(role_key) {
        Some(Json::Str(s)) if s == "all" => true,
        Some(Json::Array(arr)) => arr.iter().any(|v| v.as_str() == Some(lang)),
        _ => true,
    }
}

fn docker_run_cmdline(
    cmdline: &[String],
    image: &str,
    docker_args: &[String],
    cwd: Option<&str>,
    environ: &[(String, String)],
    workdir_root: &str,
) -> Vec<String> {
    let mut out = vec![
        "docker".to_string(),
        "run".to_string(),
        "-i".to_string(),
        "--rm=true".to_string(),
    ];
    for (k, v) in environ {
        out.push("-e".to_string());
        out.push(format!("{}={}", k, v));
    }
    let workdir = match cwd {
        Some(c) if !c.is_empty() => {
            if c.starts_with('/') {
                c.to_string()
            } else {
                format!("{}/{}", workdir_root, c)
            }
        }
        _ => workdir_root.to_string(),
    };
    out.push("-w".to_string());
    out.push(workdir);
    out.extend_from_slice(docker_args);
    out.push(image.to_string());
    out.extend_from_slice(cmdline);
    out
}

fn manual_cmdline(docker_cmdline: &[String], docker_image: &str) -> String {
    let mut parts = Vec::new();
    for item in docker_cmdline {
        if item.starts_with("--name=") {
            continue;
        }
        let mut s = if item == docker_image {
            "$docker_image".to_string()
        } else {
            item.clone()
        };
        s = s.replace('"', "\\\"");
        if s.chars().any(|c| c.is_whitespace()) {
            s = format!("\"{}\"", s);
        }
        parts.push(s);
    }
    parts.join(" ")
}

pub fn generate_plan_json(root: &Json, args: &CliArgs) -> Result<Json, String> {
    let defaults = root.get("defaults").unwrap();
    let workdir_root = defaults
        .get_str("docker_workdir_root")
        .unwrap_or("/var/local/git/grpc");
    let default_port = defaults.get_i64("default_server_port", 8080);
    let prod_port = defaults.get_i64("prod_server_port", 443);
    let test_timeout = defaults.get_i64("test_timeout_seconds", 180);
    let server_timeout = defaults.get_i64("server_timeout_seconds", 1800);
    let build_timeout = defaults.get_i64("build_image_timeout_seconds", 2700);
    let flake_retries = if args.allow_flakes {
        defaults.get_i64("flake_retries", 4)
    } else {
        0
    };
    let timeout_retries = if args.allow_flakes {
        defaults.get_i64("timeout_retries", 2)
    } else {
        0
    };
    let key_file = args.service_account_key_file.clone().unwrap_or_else(|| {
        defaults
            .get_str("default_service_account_key_file")
            .unwrap_or("")
            .to_string()
    });
    let default_sa = args.default_service_account.clone().unwrap_or_else(|| {
        defaults
            .get_str("default_service_account")
            .unwrap_or("")
            .to_string()
    });
    let oauth_scope = defaults.get_str("oauth_scope").unwrap_or("");
    let custom_creds = if args.custom_credentials_type.is_empty() {
        root.get("custom_credentials_types")
            .unwrap()
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>()
    } else {
        args.custom_credentials_type.clone()
    };

    let langs_obj = root.get("languages").unwrap().as_object().unwrap();
    let lang_map: HashMap<String, &Json> =
        langs_obj.iter().map(|(k, v)| (k.clone(), v)).collect();
    let http2_target_name = find_http2_target_name(root);
    let sec_sec = root.get("transport_security").unwrap();
    let active_sec = sec_sec
        .get(&args.transport_security)
        .ok_or_else(|| format!("Invalid transport_security '{}'", args.transport_security))?;

    // Resolve client languages
    let mut selected_langs = BTreeSet::new();
    for l in &args.languages {
        if l == "all" {
            for (name, ldata) in langs_obj {
                if !ldata.get_bool("is_http2_target", false)
                    && ldata.get_bool("included_in_all_clients", true)
                {
                    selected_langs.insert(name.clone());
                }
            }
        } else if lang_map.contains_key(l) {
            selected_langs.insert(l.clone());
        } else {
            return Err(format!("Unknown language '{}'", l));
        }
    }
    selected_langs.retain(|l| is_supported_for_sec(active_sec, "supported_clients", l));
    let languages: Vec<String> = selected_langs.into_iter().collect();

    // Resolve servers (filtering out idle servers with 0 supported server_test_cases)
    let mut selected_servers = BTreeSet::new();
    for s in &args.servers {
        if s == "all" {
            for (name, ldata) in langs_obj {
                if !ldata.get_bool("is_http2_target", false)
                    && ldata.get_bool("supports_server", false)
                    && !ldata.get_string_vec("server_test_cases").is_empty()
                {
                    selected_servers.insert(name.clone());
                }
            }
        } else if let Some(ldata) = lang_map.get(s) {
            if !ldata.get_string_vec("server_test_cases").is_empty() {
                selected_servers.insert(s.clone());
            }
        } else {
            return Err(format!("Unknown server '{}'", s));
        }
    }
    selected_servers.retain(|s| is_supported_for_sec(active_sec, "supported_servers", s));
    let servers: Vec<String> = selected_servers.into_iter().collect();

    // Resolve http2_clients for http2_server_interop
    let mut http2_clients = Vec::new();
    if args.http2_server_interop {
        let supported_h2 = root
            .get("suites")
            .unwrap()
            .get("http2_server")
            .unwrap()
            .get_string_vec("supported_http2_clients");
        let mut h2_set = BTreeSet::new();
        for l in &supported_h2 {
            if args.languages.iter().any(|x| x == "all" || x == l) {
                h2_set.insert(l.clone());
            }
        }
        http2_clients = h2_set.into_iter().collect();
    }

    // Deduplicated Docker image build plan
    let mut build_jobs = Vec::new();
    let mut docker_images: BTreeMap<String, String> = BTreeMap::new();
    if args.use_docker {
        let mut requested: Vec<String> = Vec::new();
        for l in &languages {
            if !requested.contains(l) {
                requested.push(l.clone());
            }
        }
        for s in &servers {
            if !requested.contains(s) {
                requested.push(s.clone());
            }
        }
        for hc in &http2_clients {
            if !requested.contains(hc) {
                requested.push(hc.clone());
            }
        }
        if (args.http2_interop || args.http2_server_interop)
            && !http2_target_name.is_empty()
            && !requested.contains(&http2_target_name)
        {
            requested.push(http2_target_name.clone());
        }
        requested.sort_by(|a, b| {
            let sa = lang_map[a].get_str("safename").unwrap_or("");
            let sb = lang_map[b].get_str("safename").unwrap_or("");
            let ca = if a == sa { 0 } else { 1 };
            let cb = if b == sb { 0 } else { 1 };
            (sa, ca, a).cmp(&(sb, cb, b))
        });

        let mut safename_to_tag: HashMap<String, String> = HashMap::new();
        let builder_script = defaults
            .get_str("legacy_image_builder_script")
            .unwrap_or("tools/run_tests/dockerize/build_interop_image.sh");

        for (idx, tname) in requested.iter().enumerate() {
            let ldata = lang_map[tname];
            if !ldata.get_bool("docker_supported", true) {
                continue;
            }
            let safename = ldata.get_str("safename").unwrap_or(tname);
            if let Some(existing_tag) = safename_to_tag.get(safename) {
                docker_images.insert(tname.clone(), existing_tag.clone());
                continue;
            }
            let tag = format!("grpc_interop_{}:{}", safename, pseudo_uuid(idx + 1));
            safename_to_tag.insert(safename.to_string(), tag.clone());
            docker_images.insert(tname.clone(), tag.clone());
            build_jobs.push(Json::Object(vec![
                ("target".to_string(), Json::Str(tname.clone())),
                ("safename".to_string(), Json::Str(safename.to_string())),
                ("tag".to_string(), Json::Str(tag.clone())),
                (
                    "shortname".to_string(),
                    Json::Str(format!("build_docker_{}", tname)),
                ),
                (
                    "cmdline".to_string(),
                    Json::Array(vec![Json::Str(builder_script.to_string())]),
                ),
                (
                    "environ".to_string(),
                    Json::Object(vec![
                        ("INTEROP_IMAGE".to_string(), Json::Str(tag)),
                        (
                            "BASE_NAME".to_string(),
                            Json::Str(format!("grpc_interop_{}", safename)),
                        ),
                    ]),
                ),
                (
                    "timeout_seconds".to_string(),
                    Json::Number(build_timeout as f64),
                ),
            ]));
        }
    }

    // Server specs and manual log
    let mut server_manual_log: Vec<String> = Vec::new();
    let mut client_manual_log: Vec<String> = Vec::new();
    let mut server_specs = Vec::new();
    let mut server_addresses: Vec<(String, String, String)> = Vec::new();
    let mut seed_counter = 100usize;

    let build_server_spec = |sname: &str,
                             ldata: &Json,
                             sec_name: &str,
                             image: &str,
                             seed: usize,
                             manual_log: &mut Vec<String>|
     -> Result<Json, String> {
        let safename = ldata.get_str("safename").unwrap_or(sname);
        let cname = format!("interop_server_{}_{}", safename, pseudo_uuid(seed));
        let mut srv_args = vec![format!("--port={}", default_port)];
        let s_sec = sec_sec
            .get(sec_name)
            .ok_or_else(|| format!("Invalid transport_security '{}'", sec_name))?;
        srv_args.extend(s_sec.get_string_vec("server_flags"));
        let srv_cmd_obj = ldata.get("server_cmd").ok_or("Missing server_cmd")?;
        let rendered = render_command(srv_cmd_obj, &srv_args).unwrap_or_default();
        let bash_cmd = vec![
            "bash".to_string(),
            "-c".to_string(),
            rendered.join(" "),
        ];
        let env_pairs = ldata.get_string_map("global_env");
        let mut d_args = vec![format!("--name={}", cname)];
        let h2_cases_len = root
            .get("test_cases")
            .unwrap()
            .get("http2_server")
            .unwrap()
            .as_array()
            .unwrap()
            .len();
        if let Some(sdc) = ldata.get("server_docker_config") {
            if *sdc != Json::Null {
                let pcount = if sdc.get_str("port_count_from_suite") == Some("http2_server") {
                    h2_cases_len
                } else {
                    sdc.get_i64("port_count", 1) as usize
                };
                for p_idx in 0..pcount {
                    d_args.push("-p".to_string());
                    d_args.push((default_port + p_idx as i64).to_string());
                }
                if let Some(hc) = sdc.get("healthcheck") {
                    if *hc != Json::Null {
                        let cmd_t = hc.get_str("cmd_template").unwrap_or("");
                        d_args.push(format!(
                            "--health-cmd={}",
                            cmd_t.replace("{port}", &default_port.to_string())
                        ));
                        d_args.push(format!(
                            "--health-interval={}",
                            hc.get_str("interval").unwrap_or("1s")
                        ));
                        d_args.push(format!(
                            "--health-retries={}",
                            hc.get_i64("retries", 5)
                        ));
                        d_args.push(format!(
                            "--health-timeout={}",
                            hc.get_str("timeout").unwrap_or("10s")
                        ));
                    }
                }
            } else {
                d_args.push("-p".to_string());
                d_args.push(default_port.to_string());
            }
        } else {
            d_args.push("-p".to_string());
            d_args.push(default_port.to_string());
        }
        let srv_cwd = ldata.get_str("server_cwd");
        let full_cmd =
            docker_run_cmdline(&bash_cmd, image, &d_args, srv_cwd, &env_pairs, workdir_root);
        if args.manual_run {
            if manual_log.is_empty() {
                manual_log.push(format!("echo \"Testing ${{docker_image:={}}}\"", image));
            }
            manual_log.push(manual_cmdline(&full_cmd, image));
        }
        Ok(Json::Object(vec![
            ("server".to_string(), Json::Str(sname.to_string())),
            (
                "shortname".to_string(),
                Json::Str(format!("interop_server_{}", sname)),
            ),
            ("container_name".to_string(), Json::Str(cname)),
            (
                "cmdline".to_string(),
                Json::Array(full_cmd.into_iter().map(Json::Str).collect()),
            ),
            (
                "environ".to_string(),
                Json::Object(
                    env_pairs
                        .into_iter()
                        .map(|(k, v)| (k, Json::Str(v)))
                        .collect(),
                ),
            ),
            (
                "timeout_seconds".to_string(),
                Json::Number(server_timeout as f64),
            ),
        ]))
    };

    for s in &servers {
        seed_counter += 1;
        let ldata = lang_map[s];
        let img = docker_images.get(s).map(|s| s.as_str()).unwrap_or("");
        let spec_json = build_server_spec(
            s,
            ldata,
            &args.transport_security,
            img,
            seed_counter,
            &mut server_manual_log,
        )?;
        server_specs.push((s.clone(), spec_json));
        let port_str = if args.manual_run {
            "${SERVER_PORT}".to_string()
        } else {
            default_port.to_string()
        };
        server_addresses.push((s.clone(), "localhost".to_string(), port_str));
    }

    if args.http2_server_interop && !http2_target_name.is_empty() {
        seed_counter += 1;
        let ldata = lang_map[&http2_target_name];
        let img = docker_images
            .get(&http2_target_name)
            .map(|s| s.as_str())
            .unwrap_or("");
        let spec_json = build_server_spec(
            &http2_target_name,
            ldata,
            "tls",
            img,
            seed_counter,
            &mut server_manual_log,
        )?;
        server_specs.push((http2_target_name.clone(), spec_json));
        if args.manual_run {
            server_addresses.push((
                http2_target_name.clone(),
                "localhost".to_string(),
                "${SERVER_PORT}".to_string(),
            ));
        }
    }

    for (srv_name, host_port) in &args.override_servers {
        if let Some((h, p)) = host_port.split_once(':') {
            if let Some(entry) = server_addresses.iter_mut().find(|(n, _, _)| n == srv_name) {
                entry.1 = h.to_string();
                entry.2 = p.to_string();
            } else {
                server_addresses.push((srv_name.clone(), h.to_string(), p.to_string()));
            }
        }
    }

    // Build client jobs
    let mut client_jobs = Vec::new();
    let mut skipped_tests = Vec::new();
    let tc_sec = root.get("test_cases").unwrap();
    let std_specs = tc_sec.get("standard").unwrap().as_array().unwrap();
    let auth_specs = tc_sec.get("auth").unwrap().as_array().unwrap();
    let h2_cli_specs = tc_sec.get("http2_client").unwrap().as_array().unwrap();
    let h2_srv_specs = tc_sec.get("http2_server").unwrap().as_array().unwrap();
    let prod_servers_map = root.get("prod_servers").unwrap();
    let creds_sec = root.get("custom_credentials_types").unwrap();

    let make_c2p_job = |lname: &str,
                        ldata: &Json,
                        test_case: &str,
                        nick: &str,
                        host: &str,
                        sec_type: &str,
                        is_auth: bool,
                        auth_spec_opt: Option<&Json>,
                        docker_img: Option<&String>,
                        seed: usize,
                        manual_log: &mut Vec<String>|
     -> Result<Json, String> {
        let mut cmdargs = vec![
            format!("--server_host={}", host),
            format!("--server_port={}", prod_port),
            format!("--test_case={}", test_case),
        ];
        let cred_obj = creds_sec
            .get(sec_type)
            .ok_or_else(|| format!("Unknown custom_credentials_type '{}'", sec_type))?;
        cmdargs.extend(cred_obj.get_string_vec("client_flags"));
        let mut env_map: BTreeMap<String, String> = BTreeMap::new();
        for (k, v) in ldata.get_string_map("cloud_to_prod_env") {
            env_map.insert(k, v);
        }
        for (k, v) in ldata.get_string_map("global_env") {
            env_map.insert(k, v);
        }
        if is_auth {
            if let Some(aspec) = auth_spec_opt {
                if aspec.get_bool("uses_service_account_key", false) {
                    if ldata.get_str("auth_credentials_mode") == Some("env") {
                        env_map.insert(
                            "GOOGLE_APPLICATION_CREDENTIALS".to_string(),
                            key_file.clone(),
                        );
                    } else {
                        cmdargs.push(format!("--service_account_key_file={}", key_file));
                    }
                }
                if aspec.get_bool("uses_oauth_scope", false) {
                    cmdargs.push(format!("--oauth_scope={}", oauth_scope));
                }
                if aspec.get_str("uses_service_account_key_when_flag")
                    == Some("google_default_creds_use_key_file")
                    && args.google_default_creds_use_key_file
                {
                    env_map.insert(
                        "GOOGLE_APPLICATION_CREDENTIALS".to_string(),
                        key_file.clone(),
                    );
                }
                let needs_sa = aspec.get_bool("uses_default_service_account", false)
                    || aspec
                        .get_string_vec("uses_default_service_account_languages")
                        .iter()
                        .any(|x| x == lname)
                    || (test_case == "oauth2_auth_token"
                        && ldata.get_bool("oauth2_uses_gce_creds", false));
                if needs_sa {
                    cmdargs.push(format!("--default_service_account={}", default_sa));
                }
            }
        }
        let c_cmd_obj = ldata.get("client_cmd").ok_or("Missing client_cmd")?;
        let rendered = render_command(c_cmd_obj, &cmdargs).unwrap_or_default();
        let mut cmdline = vec![
            "bash".to_string(),
            "-c".to_string(),
            rendered.join(" "),
        ];
        let mut cwd_opt: Option<String> = ldata.get_str("client_cwd").map(|s| s.to_string());
        let env_vec: Vec<(String, String)> =
            env_map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let mut cname_opt: Option<String> = None;
        let mut final_env_opt: Option<Vec<(String, Json)>> = Some(
            env_map
                .into_iter()
                .map(|(k, v)| (k, Json::Str(v)))
                .collect(),
        );

        if let Some(img) = docker_img {
            let safename = ldata.get_str("safename").unwrap_or(lname);
            let cname = format!("interop_client_{}_{}", safename, pseudo_uuid(seed));
            let d_args = vec!["--net=host".to_string(), format!("--name={}", cname)];
            cmdline = docker_run_cmdline(
                &cmdline,
                img,
                &d_args,
                cwd_opt.as_deref(),
                &env_vec,
                workdir_root,
            );
            if args.manual_run {
                if manual_log.is_empty() {
                    manual_log.push(format!("echo \"Testing ${{docker_image:={}}}\"", img));
                }
                manual_log.push(manual_cmdline(&cmdline, img));
            }
            cwd_opt = None;
            final_env_opt = None;
            cname_opt = Some(cname);
        }
        let suite = if is_auth {
            "cloud_to_prod_auth"
        } else {
            "cloud_to_prod"
        };
        let shortname = format!("{}:{}:{}:{}:{}", suite, lname, nick, test_case, sec_type);
        Ok(Json::Object(vec![
            ("shortname".to_string(), Json::Str(shortname)),
            (
                "cmdline".to_string(),
                Json::Array(cmdline.into_iter().map(Json::Str).collect()),
            ),
            (
                "cwd".to_string(),
                cwd_opt.map(Json::Str).unwrap_or(Json::Null),
            ),
            (
                "environ".to_string(),
                final_env_opt.map(Json::Object).unwrap_or(Json::Null),
            ),
            (
                "container_name".to_string(),
                cname_opt.map(Json::Str).unwrap_or(Json::Null),
            ),
            (
                "timeout_seconds".to_string(),
                Json::Number(test_timeout as f64),
            ),
            (
                "flake_retries".to_string(),
                Json::Number(flake_retries as f64),
            ),
            (
                "timeout_retries".to_string(),
                Json::Number(timeout_retries as f64),
            ),
        ]))
    };

    if args.cloud_to_prod {
        for nick in &args.prod_servers {
            let host = prod_servers_map
                .get_str(nick)
                .ok_or_else(|| format!("Unknown prod_server '{}'", nick))?;
            for lname in &languages {
                let ldata = lang_map[lname];
                let unimpl = lang_unimplemented_client(root, ldata, false);
                for tc_obj in std_specs {
                    let tc_name = tc_obj.get_str("name").unwrap_or("");
                    if unimpl.iter().any(|u| u == tc_name) {
                        skipped_tests.push(Json::Object(vec![
                            ("suite".to_string(), Json::Str("cloud_to_prod".to_string())),
                            ("client".to_string(), Json::Str(lname.clone())),
                            ("server".to_string(), Json::Str(nick.clone())),
                            ("test_case".to_string(), Json::Str(tc_name.to_string())),
                            (
                                "reason".to_string(),
                                Json::Str(format!("unimplemented by client '{}'", lname)),
                            ),
                        ]));
                        continue;
                    }
                    if !tc_obj.get_bool("cloud_to_prod_eligible", true) {
                        skipped_tests.push(Json::Object(vec![
                            ("suite".to_string(), Json::Str("cloud_to_prod".to_string())),
                            ("client".to_string(), Json::Str(lname.clone())),
                            ("server".to_string(), Json::Str(nick.clone())),
                            ("test_case".to_string(), Json::Str(tc_name.to_string())),
                            (
                                "reason".to_string(),
                                Json::Str("not cloud_to_prod_eligible".to_string()),
                            ),
                        ]));
                        continue;
                    }
                    for cred in &custom_creds {
                        let cred_obj = creds_sec.get(cred).unwrap();
                        if !is_supported_for_sec(cred_obj, "supported_clients", lname) {
                            skipped_tests.push(Json::Object(vec![
                                ("suite".to_string(), Json::Str("cloud_to_prod".to_string())),
                                ("client".to_string(), Json::Str(lname.clone())),
                                ("server".to_string(), Json::Str(nick.clone())),
                                (
                                    "test_case".to_string(),
                                    Json::Str(format!("{}:{}", tc_name, cred)),
                                ),
                                (
                                    "reason".to_string(),
                                    Json::Str(format!(
                                        "client '{}' does not support custom_credentials_type '{}'",
                                        lname, cred
                                    )),
                                ),
                            ]));
                            continue;
                        }
                        seed_counter += 1;
                        let job = make_c2p_job(
                            lname,
                            ldata,
                            tc_name,
                            nick,
                            host,
                            cred,
                            false,
                            None,
                            docker_images.get(lname),
                            seed_counter,
                            &mut client_manual_log,
                        )?;
                        client_jobs.push(job);
                    }
                }
            }
            if args.http2_interop && !http2_target_name.is_empty() {
                let h2_data = lang_map[&http2_target_name];
                for tc_obj in h2_cli_specs {
                    let tc_name = tc_obj.get_str("name").unwrap_or("");
                    seed_counter += 1;
                    let job = make_c2p_job(
                        &http2_target_name,
                        h2_data,
                        tc_name,
                        nick,
                        host,
                        &args.transport_security,
                        false,
                        None,
                        docker_images.get(&http2_target_name),
                        seed_counter,
                        &mut client_manual_log,
                    )?;
                    client_jobs.push(job);
                }
            }
        }
    }

    if args.cloud_to_prod_auth {
        for nick in &args.prod_servers {
            let host = prod_servers_map
                .get_str(nick)
                .ok_or_else(|| format!("Unknown prod_server '{}'", nick))?;
            for lname in &languages {
                let ldata = lang_map[lname];
                let unimpl = lang_unimplemented_client(root, ldata, false);
                for aspec in auth_specs {
                    let tc_name = aspec.get_str("name").unwrap_or("");
                    let needs_gce = aspec.get_bool("requires_compute_engine_creds", false)
                        || aspec
                            .get_string_vec("requires_compute_engine_creds_languages")
                            .iter()
                            .any(|x| x == lname)
                        || (tc_name == "oauth2_auth_token"
                            && ldata.get_bool("oauth2_uses_gce_creds", false));
                    if args.skip_compute_engine_creds && needs_gce {
                        skipped_tests.push(Json::Object(vec![
                            (
                                "suite".to_string(),
                                Json::Str("cloud_to_prod_auth".to_string()),
                            ),
                            ("client".to_string(), Json::Str(lname.clone())),
                            ("server".to_string(), Json::Str(nick.clone())),
                            ("test_case".to_string(), Json::Str(tc_name.to_string())),
                            (
                                "reason".to_string(),
                                Json::Str("skipped due to --skip_compute_engine_creds".to_string()),
                            ),
                        ]));
                        continue;
                    }
                    if unimpl.iter().any(|u| u == tc_name) {
                        skipped_tests.push(Json::Object(vec![
                            (
                                "suite".to_string(),
                                Json::Str("cloud_to_prod_auth".to_string()),
                            ),
                            ("client".to_string(), Json::Str(lname.clone())),
                            ("server".to_string(), Json::Str(nick.clone())),
                            ("test_case".to_string(), Json::Str(tc_name.to_string())),
                            (
                                "reason".to_string(),
                                Json::Str(format!("unimplemented by client '{}'", lname)),
                            ),
                        ]));
                        continue;
                    }
                    let sec_type = aspec.get_str("transport_security").unwrap_or("tls");
                    if !custom_creds.iter().any(|c| c == sec_type) {
                        skipped_tests.push(Json::Object(vec![
                            (
                                "suite".to_string(),
                                Json::Str("cloud_to_prod_auth".to_string()),
                            ),
                            ("client".to_string(), Json::Str(lname.clone())),
                            ("server".to_string(), Json::Str(nick.clone())),
                            ("test_case".to_string(), Json::Str(tc_name.to_string())),
                            (
                                "reason".to_string(),
                                Json::Str(format!(
                                    "transport_security '{}' not in --custom_credentials_type",
                                    sec_type
                                )),
                            ),
                        ]));
                        continue;
                    }
                    seed_counter += 1;
                    let job = make_c2p_job(
                        lname,
                        ldata,
                        tc_name,
                        nick,
                        host,
                        sec_type,
                        true,
                        Some(aspec),
                        docker_images.get(lname),
                        seed_counter,
                        &mut client_manual_log,
                    )?;
                    client_jobs.push(job);
                }
            }
        }
    }

    // Cloud-to-cloud and HTTP/2 suites
    let c2c_extra_flags = root
        .get("suites")
        .unwrap()
        .get("cloud_to_cloud")
        .unwrap()
        .get_string_vec("client_extra_flags");
    let skip_h2_c2c_servers: BTreeSet<String> = root
        .get("suites")
        .unwrap()
        .get("http2_client")
        .unwrap()
        .get_string_vec("skip_cloud_to_cloud_servers")
        .into_iter()
        .collect();

    let h2_srv_all_names: Vec<String> = h2_srv_specs
        .iter()
        .filter_map(|c| c.get_str("name").map(|s| s.to_string()))
        .collect();
    let mut h2_grpc_map: HashMap<String, (String, String)> = HashMap::new();
    for c in h2_srv_specs {
        if c.get_str("client_kind") == Some("grpc_client") {
            if let (Some(n), Some(gtc)) = (c.get_str("name"), c.get_str("grpc_client_test_case")) {
                let sec_ov = c
                    .get_str("transport_security_override")
                    .unwrap_or("insecure")
                    .to_string();
                h2_grpc_map.insert(n.to_string(), (gtc.to_string(), sec_ov));
            }
        }
    }

    let make_c2c_job = |lname: &str,
                        ldata: &Json,
                        test_case: &str,
                        srv_name: &str,
                        srv_host: &str,
                        srv_port: &str,
                        sec_name: &str,
                        docker_img: Option<&String>,
                        seed: usize,
                        manual_log: &mut Vec<String>|
     -> Result<Json, String> {
        let mut interop_opts = c2c_extra_flags.clone();
        let s_obj = sec_sec
            .get(sec_name)
            .ok_or_else(|| format!("Invalid transport_security '{}'", sec_name))?;
        interop_opts.extend(s_obj.get_string_vec("client_flags"));
        let client_tc = if let Some((gtc, _)) = h2_grpc_map.get(test_case) {
            gtc.as_str()
        } else {
            test_case
        };
        for sc in std_specs {
            if sc.get_str("name") == Some(test_case) {
                interop_opts.extend(sc.get_string_vec("extra_cloud_to_cloud_flags"));
            }
        }
        let common_opts = vec![
            format!("--test_case={}", client_tc),
            format!("--server_host={}", srv_host),
            format!("--server_port={}", srv_port),
        ];
        let (rendered, mut cwd_opt) = if h2_srv_all_names.iter().any(|x| x == test_case) {
            if h2_grpc_map.contains_key(test_case) {
                let mut opts = interop_opts;
                opts.extend(common_opts);
                let r = render_command(ldata.get("client_cmd").unwrap(), &opts).unwrap_or_default();
                (r, ldata.get_str("client_cwd").map(|s| s.to_string()))
            } else {
                let r = render_command(ldata.get("http2_client_cmd").unwrap(), &common_opts)
                    .unwrap_or_default();
                (r, ldata.get_str("http2_cwd").map(|s| s.to_string()))
            }
        } else {
            let mut opts = common_opts;
            opts.extend(interop_opts);
            let r = render_command(ldata.get("client_cmd").unwrap(), &opts).unwrap_or_default();
            (r, ldata.get_str("client_cwd").map(|s| s.to_string()))
        };
        let mut cmdline = vec![
            "bash".to_string(),
            "-c".to_string(),
            rendered.join(" "),
        ];
        let env_vec = ldata.get_string_map("global_env");
        let docker_ok = ldata.get_bool("docker_supported", true);
        let mut cname_opt = None;
        if let Some(img) = docker_img {
            if docker_ok {
                let safename = ldata.get_str("safename").unwrap_or(lname);
                let cname = format!("interop_client_{}_{}", safename, pseudo_uuid(seed));
                let d_args = vec!["--net=host".to_string(), format!("--name={}", cname)];
                cmdline = docker_run_cmdline(
                    &cmdline,
                    img,
                    &d_args,
                    cwd_opt.as_deref(),
                    &env_vec,
                    workdir_root,
                );
                if args.manual_run {
                    if manual_log.is_empty() {
                        manual_log.push(format!("echo \"Testing ${{docker_image:={}}}\"", img));
                    }
                    manual_log.push(manual_cmdline(&cmdline, img));
                }
                cwd_opt = None;
                cname_opt = Some(cname);
            }
        }
        let shortname = format!(
            "cloud_to_cloud:{}:{}_server:{}:{}",
            lname, srv_name, test_case, sec_name
        );
        Ok(Json::Object(vec![
            ("shortname".to_string(), Json::Str(shortname)),
            (
                "cmdline".to_string(),
                Json::Array(cmdline.into_iter().map(Json::Str).collect()),
            ),
            (
                "cwd".to_string(),
                cwd_opt.map(Json::Str).unwrap_or(Json::Null),
            ),
            (
                "environ".to_string(),
                Json::Object(
                    env_vec
                        .into_iter()
                        .map(|(k, v)| (k, Json::Str(v)))
                        .collect(),
                ),
            ),
            (
                "container_name".to_string(),
                cname_opt.map(Json::Str).unwrap_or(Json::Null),
            ),
            (
                "timeout_seconds".to_string(),
                Json::Number(test_timeout as f64),
            ),
            (
                "flake_retries".to_string(),
                Json::Number(flake_retries as f64),
            ),
            (
                "timeout_retries".to_string(),
                Json::Number(timeout_retries as f64),
            ),
        ]))
    };

    for (srv_name, srv_host, srv_port) in &server_addresses {
        let srv_ldata = lang_map.get(srv_name);
        let skip_srv = srv_ldata
            .map(|ld| lang_unimplemented_server(root, ld))
            .unwrap_or_default();
        for lname in &languages {
            let ldata = lang_map[lname];
            let unimpl_c = lang_unimplemented_client(root, ldata, false);
            for sc in std_specs {
                let tc_name = sc.get_str("name").unwrap_or("");
                if unimpl_c.iter().any(|u| u == tc_name) {
                    skipped_tests.push(Json::Object(vec![
                        (
                            "suite".to_string(),
                            Json::Str("cloud_to_cloud".to_string()),
                        ),
                        ("client".to_string(), Json::Str(lname.clone())),
                        ("server".to_string(), Json::Str(srv_name.clone())),
                        ("test_case".to_string(), Json::Str(tc_name.to_string())),
                        (
                            "reason".to_string(),
                            Json::Str(format!("unimplemented by client '{}'", lname)),
                        ),
                    ]));
                    continue;
                }
                if skip_srv.iter().any(|u| u == tc_name) {
                    skipped_tests.push(Json::Object(vec![
                        (
                            "suite".to_string(),
                            Json::Str("cloud_to_cloud".to_string()),
                        ),
                        ("client".to_string(), Json::Str(lname.clone())),
                        ("server".to_string(), Json::Str(srv_name.clone())),
                        ("test_case".to_string(), Json::Str(tc_name.to_string())),
                        (
                            "reason".to_string(),
                            Json::Str(format!("unimplemented by server '{}'", srv_name)),
                        ),
                    ]));
                    continue;
                }
                seed_counter += 1;
                let job = make_c2c_job(
                    lname,
                    ldata,
                    tc_name,
                    srv_name,
                    srv_host,
                    srv_port,
                    &args.transport_security,
                    docker_images.get(lname),
                    seed_counter,
                    &mut client_manual_log,
                )?;
                client_jobs.push(job);
            }
        }
        if args.http2_interop && !http2_target_name.is_empty() {
            let h2_data = lang_map[&http2_target_name];
            let skip_bad = srv_ldata
                .map(|ld| ld.get_bool("skip_http2_bad_client", false))
                .unwrap_or(false)
                || skip_h2_c2c_servers.contains(srv_name);
            for hc in h2_cli_specs {
                let tc_name = hc.get_str("name").unwrap_or("");
                if skip_bad {
                    skipped_tests.push(Json::Object(vec![
                        ("suite".to_string(), Json::Str("http2_client".to_string())),
                        (
                            "client".to_string(),
                            Json::Str(http2_target_name.clone()),
                        ),
                        ("server".to_string(), Json::Str(srv_name.clone())),
                        ("test_case".to_string(), Json::Str(tc_name.to_string())),
                        (
                            "reason".to_string(),
                            Json::Str(format!(
                                "server '{}' skipped for http2_client suite",
                                srv_name
                            )),
                        ),
                    ]));
                    continue;
                }
                seed_counter += 1;
                let job = make_c2c_job(
                    &http2_target_name,
                    h2_data,
                    tc_name,
                    srv_name,
                    srv_host,
                    srv_port,
                    &args.transport_security,
                    docker_images.get(&http2_target_name),
                    seed_counter,
                    &mut client_manual_log,
                )?;
                client_jobs.push(job);
            }
        }
    }

    if args.http2_server_interop && !http2_target_name.is_empty() {
        let mut sorted_h2 = h2_srv_all_names.clone();
        sorted_h2.sort();
        for lname in &http2_clients {
            let ldata = lang_map[lname];
            for tc_name in &h2_srv_all_names {
                if h2_grpc_map.contains_key(tc_name) {
                    continue;
                }
                let offset = sorted_h2.iter().position(|x| x == tc_name).unwrap_or(0);
                let port = (default_port + offset as i64).to_string();
                seed_counter += 1;
                let job = make_c2c_job(
                    lname,
                    ldata,
                    tc_name,
                    &http2_target_name,
                    "localhost",
                    &port,
                    &args.transport_security,
                    docker_images.get(lname),
                    seed_counter,
                    &mut client_manual_log,
                )?;
                client_jobs.push(job);
            }
        }
        for lname in &languages {
            let ldata = lang_map[lname];
            let unimpl_c = lang_unimplemented_client(root, ldata, false);
            for tc_name in &h2_srv_all_names {
                if let Some((_, sec_ov)) = h2_grpc_map.get(tc_name) {
                    if unimpl_c.iter().any(|u| u == tc_name) {
                        skipped_tests.push(Json::Object(vec![
                            ("suite".to_string(), Json::Str("http2_server".to_string())),
                            ("client".to_string(), Json::Str(lname.clone())),
                            (
                                "server".to_string(),
                                Json::Str(http2_target_name.clone()),
                            ),
                            ("test_case".to_string(), Json::Str(tc_name.clone())),
                            (
                                "reason".to_string(),
                                Json::Str(format!("unimplemented by client '{}'", lname)),
                            ),
                        ]));
                        continue;
                    }
                    let offset = sorted_h2.iter().position(|x| x == tc_name).unwrap_or(0);
                    let port = (default_port + offset as i64).to_string();
                    seed_counter += 1;
                    let job = make_c2c_job(
                        lname,
                        ldata,
                        tc_name,
                        &http2_target_name,
                        "localhost",
                        &port,
                        sec_ov,
                        docker_images.get(lname),
                        seed_counter,
                        &mut client_manual_log,
                    )?;
                    client_jobs.push(job);
                }
            }
        }
    }

    Ok(Json::Object(vec![
        (
            "languages".to_string(),
            Json::Array(languages.into_iter().map(Json::Str).collect()),
        ),
        (
            "servers".to_string(),
            Json::Array(servers.into_iter().map(Json::Str).collect()),
        ),
        (
            "http2_clients".to_string(),
            Json::Array(http2_clients.into_iter().map(Json::Str).collect()),
        ),
        ("build_jobs".to_string(), Json::Array(build_jobs)),
        (
            "docker_images".to_string(),
            Json::Object(
                docker_images
                    .into_iter()
                    .map(|(k, v)| (k, Json::Str(v)))
                    .collect(),
            ),
        ),
        ("server_specs".to_string(), Json::Object(server_specs)),
        ("client_jobs".to_string(), Json::Array(client_jobs)),
        ("skipped_tests".to_string(), Json::Array(skipped_tests)),
        (
            "server_manual_cmd_log".to_string(),
            if args.manual_run {
                Json::Array(server_manual_log.into_iter().map(Json::Str).collect())
            } else {
                Json::Null
            },
        ),
        (
            "client_manual_cmd_log".to_string(),
            if args.manual_run {
                Json::Array(client_manual_log.into_iter().map(Json::Str).collect())
            } else {
                Json::Null
            },
        ),
    ]))
}

pub fn print_dry_run(plan: &Json) {
    let langs = plan.get_string_vec("languages");
    let srvs = plan.get_string_vec("servers");
    let h2_clis = plan.get_string_vec("http2_clients");
    let build_jobs = plan.get("build_jobs").unwrap().as_array().unwrap();
    let srv_specs = plan.get("server_specs").unwrap().as_object().unwrap();
    let cli_jobs = plan.get("client_jobs").unwrap().as_array().unwrap();
    let skipped = plan.get("skipped_tests").unwrap().as_array().unwrap();

    println!("=== Dry-Run Execution Plan ===");
    println!(
        "Selected client languages ({}): {}",
        langs.len(),
        if langs.is_empty() {
            "none".to_string()
        } else {
            langs.join(", ")
        }
    );
    println!(
        "Selected servers ({}):          {}",
        srvs.len(),
        if srvs.is_empty() {
            "none".to_string()
        } else {
            srvs.join(", ")
        }
    );
    if !h2_clis.is_empty() {
        println!("Selected HTTP/2 clients:      {}", h2_clis.join(", "));
    }
    println!(
        "\n--- Planned Docker Image Builds ({}, deduplicated by safename) ---",
        build_jobs.len()
    );
    for bj in build_jobs {
        let short = bj.get_str("shortname").unwrap_or("");
        let tag = bj.get_str("tag").unwrap_or("");
        let cmd = bj.get_string_vec("cmdline").join(" ");
        println!("  [{}] tag={} cmd={}", short, tag, cmd);
    }
    println!("\n--- Planned Server Containers ({}) ---", srv_specs.len());
    for (sname, sj) in srv_specs {
        let cmd = sj.get_string_vec("cmdline").join(" ");
        println!("  [{}] {}", sname, cmd);
    }
    println!("\n--- Planned Client Test Jobs ({}) ---", cli_jobs.len());
    for cj in cli_jobs {
        let short = cj.get_str("shortname").unwrap_or("");
        let cmd = cj.get_string_vec("cmdline").join(" ");
        println!("  [{}] {}", short, cmd);
    }
    println!("\n--- Skipped Combinations ({}) ---", skipped.len());
}

// ============================================================================
// Standalone Execution & JUnit XML Report Generation
// ============================================================================

#[derive(Clone, Debug)]
pub struct JobOutcome {
    pub shortname: String,
    pub passed: bool,
    pub elapsed_sec: f64,
    pub output: String,
}

struct ServerContainerGuard {
    containers: Vec<String>,
}

impl Drop for ServerContainerGuard {
    fn drop(&mut self) {
        for cname in &self.containers {
            if !cname.is_empty() {
                let _ = Command::new("docker")
                    .args(["rm", "-f", cname])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
        }
    }
}

fn xml_escape(s: &str) -> String {
    let mut out = String::new();
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c if (c as u32) >= 0x20 || c == '\n' || c == '\r' || c == '\t' => out.push(c),
            _ => {}
        }
    }
    out
}

pub fn write_junit_xml_report(outcomes: &[JobOutcome], xml_report_relpath: &str) -> io::Result<()> {
    let target_path = if Path::new(xml_report_relpath).is_absolute() {
        PathBuf::from(xml_report_relpath)
    } else if let Ok(base_dir) = env::var("GRPC_TEST_REPORT_BASE_DIR") {
        if !base_dir.is_empty() {
            Path::new(&base_dir).join(xml_report_relpath)
        } else {
            PathBuf::from(xml_report_relpath)
        }
    } else {
        PathBuf::from(xml_report_relpath)
    };

    if let Some(parent) = target_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let mut xml = String::new();
    xml.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.push_str("<testsuites>\n");
    xml.push_str("  <testsuite name=\"InteropTests\" package=\"grpc_interop\">\n");
    for item in outcomes {
        let _ = writeln!(
            xml,
            "    <testcase name=\"{}\" time=\"{:.3}\">",
            xml_escape(&item.shortname),
            item.elapsed_sec
        );
        if !item.passed {
            let _ = writeln!(
                xml,
                "      <failure message=\"Failure\">{}</failure>",
                xml_escape(&item.output)
            );
        }
        xml.push_str("    </testcase>\n");
    }
    xml.push_str("  </testsuite>\n");
    xml.push_str("</testsuites>\n");
    fs::write(target_path, xml)
}

fn run_single_job(job: &Json) -> JobOutcome {
    let shortname = job.get_str("shortname").unwrap_or("job").to_string();
    let cmdline = job.get_string_vec("cmdline");
    let cwd_opt = job.get_str("cwd");
    let env_pairs = job.get_string_map("environ");
    let retries = job.get_i64("flake_retries", 0).max(0) as usize;

    let start = Instant::now();
    let mut last_out = String::new();
    for _attempt in 0..=retries {
        if cmdline.is_empty() {
            break;
        }
        let mut cmd = Command::new(&cmdline[0]);
        cmd.args(&cmdline[1..]);
        if let Some(cwd) = cwd_opt {
            if !cwd.is_empty() {
                cmd.current_dir(cwd);
            }
        }
        for (k, v) in &env_pairs {
            cmd.env(k, v);
        }
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        match cmd.output() {
            Ok(out) => {
                let mut combined = String::from_utf8_lossy(&out.stdout).to_string();
                combined.push_str(&String::from_utf8_lossy(&out.stderr));
                last_out = combined;
                if out.status.success() {
                    return JobOutcome {
                        shortname,
                        passed: true,
                        elapsed_sec: start.elapsed().as_secs_f64(),
                        output: last_out,
                    };
                }
            }
            Err(e) => {
                last_out = format!("Failed to spawn {:?}: {}", cmdline, e);
            }
        }
    }
    if let Some(cname) = job.get_str("container_name") {
        if !cname.is_empty() {
            let _ = Command::new("docker")
                .args(["rm", "-f", cname])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
    JobOutcome {
        shortname,
        passed: false,
        elapsed_sec: start.elapsed().as_secs_f64(),
        output: last_out,
    }
}

fn run_job_batch(jobs: &[Json], max_workers: usize) -> (usize, Vec<JobOutcome>) {
    if jobs.is_empty() {
        return (0, Vec::new());
    }
    let queue = Arc::new(Mutex::new(jobs.iter().cloned().collect::<Vec<_>>()));
    let results = Arc::new(Mutex::new(Vec::new()));
    let workers = max_workers.max(1).min(jobs.len());
    let mut handles = Vec::new();
    for _ in 0..workers {
        let q = Arc::clone(&queue);
        let r = Arc::clone(&results);
        handles.push(thread::spawn(move || loop {
            let next_job = {
                let mut guard = q.lock().unwrap();
                if guard.is_empty() {
                    None
                } else {
                    Some(guard.remove(0))
                }
            };
            match next_job {
                Some(j) => {
                    let outcome = run_single_job(&j);
                    r.lock().unwrap().push(outcome);
                }
                None => break,
            }
        }));
    }
    for h in handles {
        let _ = h.join();
    }
    let final_outcomes = results.lock().unwrap().clone();
    let failures = final_outcomes.iter().filter(|o| !o.passed).count();
    (failures, final_outcomes)
}

fn write_cmdlog_file(lines: &[String], filename: &str) -> io::Result<()> {
    if lines.is_empty() {
        return Ok(());
    }
    let mut f = fs::File::create(filename)?;
    writeln!(f, "#!/bin/bash")?;
    writeln!(f, "# DO NOT MODIFY")?;
    writeln!(
        f,
        "# This file is generated by run_interop_tests.py/create_testcases.sh"
    )?;
    for line in lines {
        writeln!(f, "{}", line)?;
    }
    println!("Command log written to file {}", filename);
    Ok(())
}

pub fn execute_standalone(root: &Json, args: &CliArgs) -> i32 {
    if args.manual_run && !args.use_docker {
        println!("--manual_run is only supported with --use_docker option enabled.");
        return 1;
    }
    let plan = match generate_plan_json(root, args) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Error building execution plan: {}", e);
            return 1;
        }
    };
    let servers = plan.get_string_vec("servers");
    if !args.use_docker && !servers.is_empty() {
        println!("Running interop servers is only supported with --use_docker option enabled.");
        return 1;
    }

    let reports = root.get("defaults").unwrap().get("reports").unwrap();
    let build_jobs = plan.get("build_jobs").unwrap().as_array().unwrap();
    if args.use_docker && !build_jobs.is_empty() {
        let (failures, outcomes) = run_job_batch(build_jobs, args.jobs);
        let _ = write_junit_xml_report(
            &outcomes,
            reports
                .get_str("docker_build_xml")
                .unwrap_or("interop_docker_build/sponge_log.xml"),
        );
        if failures > 0 {
            eprintln!("Failed to build interop docker images.");
            return 1;
        }
    }

    let client_jobs = plan.get("client_jobs").unwrap().as_array().unwrap();
    if client_jobs.is_empty() {
        println!("No jobs to run.");
        return 1;
    }

    if args.manual_run {
        println!("All tests will skipped --manual_run option is active.");
        let srv_log = plan.get_string_vec("server_manual_cmd_log");
        let cli_log = plan.get_string_vec("client_manual_cmd_log");
        let _ = write_cmdlog_file(
            &srv_log,
            reports
                .get_str("manual_server_cmds")
                .unwrap_or("interop_server_cmds.sh"),
        );
        let _ = write_cmdlog_file(
            &cli_log,
            reports
                .get_str("manual_client_cmds")
                .unwrap_or("interop_client_cmds.sh"),
        );
        let skipped_outcomes: Vec<JobOutcome> = client_jobs
            .iter()
            .map(|j| JobOutcome {
                shortname: j.get_str("shortname").unwrap_or("job").to_string(),
                passed: true,
                elapsed_sec: 0.0,
                output: String::new(),
            })
            .collect();
        let _ = write_junit_xml_report(
            &skipped_outcomes,
            reports
                .get_str("tests_xml")
                .unwrap_or("interop_test/sponge_log.xml"),
        );
        return 0;
    }

    let mut started_containers = Vec::new();
    if args.use_docker {
        if let Some(srv_specs) = plan.get("server_specs").and_then(|v| v.as_object()) {
            for (_sname, sjob) in srv_specs {
                if let Some(cname) = sjob.get_str("container_name") {
                    started_containers.push(cname.to_string());
                }
                let mut cmdline = sjob.get_string_vec("cmdline");
                if cmdline.len() >= 2 && cmdline[0] == "docker" && cmdline[1] == "run" {
                    cmdline.insert(2, "-d".to_string());
                    let _ = Command::new(&cmdline[0])
                        .args(&cmdline[1..])
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status();
                }
            }
        }
    }
    let _server_guard = ServerContainerGuard {
        containers: started_containers,
    };

    let (failures, outcomes) = run_job_batch(client_jobs, args.jobs);
    let _ = write_junit_xml_report(
        &outcomes,
        reports
            .get_str("tests_xml")
            .unwrap_or("interop_test/sponge_log.xml"),
    );
    if failures > 0 {
        1
    } else {
        0
    }
}

fn main() {
    let args = match CliArgs::parse_from_env() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(2);
        }
    };
    let raw_text = match fs::read_to_string(&args.config_path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Failed to read config {:?}: {}", args.config_path, e);
            std::process::exit(1);
        }
    };
    let root = match parse_json(&raw_text) {
        Ok(j) => j,
        Err(e) => {
            eprintln!("Failed to parse JSON in {:?}: {}", args.config_path, e);
            std::process::exit(1);
        }
    };
    if let Err(e) = validate_config(&root) {
        eprintln!("Config validation failed: {}", e);
        std::process::exit(1);
    }
    if args.validate_only {
        println!("OK: {:?} passed schema and cross-reference validation.", args.config_path);
        std::process::exit(0);
    }
    if args.list_languages {
        print_languages_table(&root);
        std::process::exit(0);
    }
    if args.list_tests {
        print_tests_catalog(&root);
        std::process::exit(0);
    }
    if let Some(ref target) = args.explain_matrix {
        print_explain_matrix(&root, target, &args.transport_security);
        std::process::exit(0);
    }
    if let Some(ref profile) = args.render_profile {
        match render_matrix_profile(&root, profile) {
            Ok(lines) => {
                for line in lines {
                    println!("{}", line);
                }
                std::process::exit(0);
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }
    if args.plan_json {
        match generate_plan_json(&root, &args) {
            Ok(plan) => {
                println!("{}", plan.to_json_string());
                std::process::exit(0);
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }
    if args.dry_run {
        match generate_plan_json(&root, &args) {
            Ok(plan) => {
                print_dry_run(&plan);
                std::process::exit(0);
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }
    std::process::exit(execute_standalone(&root, &args));
}
