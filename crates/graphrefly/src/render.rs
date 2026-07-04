//! Pure renderers over `DescribeSnapshot` (D39/D40).

use std::collections::{BTreeMap, BTreeSet};

use crate::graph::{DescribeEdge, DescribeNode, DescribeSnapshot, DescribeValue};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
/// `DiagramDirection` variants.
pub enum DiagramDirection {
    /// `Td` variant.
    Td,
    #[default]
    /// `Lr` variant.
    Lr,
    /// `Bt` variant.
    Bt,
    /// `Rl` variant.
    Rl,
}

impl DiagramDirection {
    fn mermaid(self) -> &'static str {
        match self {
            Self::Td => "TD",
            Self::Lr => "LR",
            Self::Bt => "BT",
            Self::Rl => "RL",
        }
    }

    fn d2(self) -> &'static str {
        match self {
            Self::Td => "down",
            Self::Lr => "right",
            Self::Bt => "up",
            Self::Rl => "left",
        }
    }
}

/// Creates or computes `describe_to_mermaid`.
pub fn describe_to_mermaid(snapshot: &DescribeSnapshot) -> String {
    describe_to_mermaid_with_direction(snapshot, DiagramDirection::default())
}

/// Creates or computes `describe_to_mermaid_with_direction`.
pub fn describe_to_mermaid_with_direction(
    snapshot: &DescribeSnapshot,
    direction: DiagramDirection,
) -> String {
    let (nodes, edges) = flatten(snapshot);
    let nodes = sorted_nodes(nodes);
    let mut ids = BTreeMap::new();
    for (i, node) in nodes.iter().enumerate() {
        ids.insert(node.id.clone(), format!("n{i}"));
    }
    let mut lines = vec![format!("flowchart {}", direction.mermaid())];
    for node in &nodes {
        lines.push(format!(
            "  {}[\"{}\"]",
            ids.get(&node.id).expect("node id present"),
            escape_quoted(&node.id)
        ));
    }
    for edge in sorted_edges(edges) {
        if let (Some(from), Some(to)) = (ids.get(&edge.from), ids.get(&edge.to)) {
            lines.push(format!("  {from} --> {to}"));
        }
    }
    lines.join("\n")
}

/// Creates or computes `mermaid_live_url`.
pub fn mermaid_live_url(source: &str) -> String {
    let payload = format!(
        "{{\"autoSync\":true,\"code\":\"{}\",\"mermaid\":{{\"theme\":\"default\"}}}}",
        json_escape(source)
    );
    format!(
        "https://mermaid.live/edit#base64:{}",
        base64_url_encode(payload.as_bytes())
    )
}

/// Creates or computes `describe_to_mermaid_url`.
pub fn describe_to_mermaid_url(snapshot: &DescribeSnapshot) -> String {
    mermaid_live_url(&describe_to_mermaid(snapshot))
}

/// Creates or computes `describe_to_d2`.
pub fn describe_to_d2(snapshot: &DescribeSnapshot) -> String {
    describe_to_d2_with_direction(snapshot, DiagramDirection::default())
}

/// Creates or computes `describe_to_d2_with_direction`.
pub fn describe_to_d2_with_direction(
    snapshot: &DescribeSnapshot,
    direction: DiagramDirection,
) -> String {
    let (nodes, edges) = flatten(snapshot);
    let nodes = sorted_nodes(nodes);
    let mut ids = BTreeMap::new();
    for (i, node) in nodes.iter().enumerate() {
        ids.insert(node.id.clone(), format!("n{i}"));
    }
    let mut lines = vec![format!("direction: {}", direction.d2())];
    for node in &nodes {
        lines.push(format!(
            "{}: \"{}\"",
            ids.get(&node.id).expect("node id present"),
            escape_quoted(&node.id)
        ));
    }
    for edge in sorted_edges(edges) {
        if let (Some(from), Some(to)) = (ids.get(&edge.from), ids.get(&edge.to)) {
            lines.push(format!("{from} -> {to}"));
        }
    }
    lines.join("\n")
}

/// Creates or computes `describe_to_pretty`.
pub fn describe_to_pretty(snapshot: &DescribeSnapshot) -> String {
    let (nodes, edges) = flatten(snapshot);
    let mut lines = vec![
        format!(
            "Graph {}",
            snapshot.name.as_deref().unwrap_or("(anonymous)")
        ),
        "Nodes:".to_owned(),
    ];
    for node in sorted_nodes(nodes) {
        lines.push(format!(
            "- {} ({}/{:?}): {}",
            node.id,
            node.factory,
            node.status,
            format_value(&node)
        ));
    }
    lines.push("Edges:".to_owned());
    for edge in sorted_edges(edges) {
        lines.push(format!("- {} -> {}", edge.from, edge.to));
    }
    lines.join("\n")
}

/// Creates or computes `describe_to_ascii`.
pub fn describe_to_ascii(snapshot: &DescribeSnapshot, include_values: bool) -> String {
    let (nodes, edges) = flatten(snapshot);
    let mut outgoing: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for edge in sorted_edges(edges) {
        outgoing.entry(edge.from).or_default().push(edge.to);
    }
    let mut lines = vec![format!(
        "Graph {}",
        snapshot.name.as_deref().unwrap_or("(anonymous)")
    )];
    for node in sorted_nodes(nodes) {
        let value = if include_values {
            format!(" {}", format_value(&node))
        } else {
            String::new()
        };
        let to = outgoing
            .get(&node.id)
            .map(|targets| targets.join(", "))
            .unwrap_or_else(|| "-".to_owned());
        lines.push(format!(
            "{} [{}/{:?}{}] -> {}",
            node.id, node.factory, node.status, value, to
        ));
    }
    lines.join("\n")
}

/// Creates or computes `describe_to_json`.
pub fn describe_to_json(snapshot: &DescribeSnapshot) -> String {
    let (nodes, edges) = flatten(snapshot);
    let mut out = String::new();
    out.push_str("{\n");
    if let Some(name) = &snapshot.name {
        out.push_str(&format!("  \"name\": \"{}\",\n", json_escape(name)));
    }
    out.push_str("  \"nodes\": [\n");
    let nodes = sorted_nodes(nodes);
    for (i, node) in nodes.iter().enumerate() {
        out.push_str("    ");
        out.push_str(&node_json(node));
        if i + 1 != nodes.len() {
            out.push(',');
        }
        out.push('\n');
    }
    out.push_str("  ],\n  \"edges\": [\n");
    let edges = sorted_edges(edges);
    for (i, edge) in edges.iter().enumerate() {
        out.push_str(&format!(
            "    {{\"from\":\"{}\",\"to\":\"{}\"}}",
            json_escape(&edge.from),
            json_escape(&edge.to)
        ));
        if i + 1 != edges.len() {
            out.push(',');
        }
        out.push('\n');
    }
    out.push_str("  ]\n}");
    out
}

fn flatten(snapshot: &DescribeSnapshot) -> (Vec<DescribeNode>, Vec<DescribeEdge>) {
    let mut nodes = snapshot.nodes.clone();
    let mut edges = snapshot.edges.clone();
    for child in snapshot.subgraphs.iter().flatten() {
        let (mut child_nodes, mut child_edges) = flatten(child);
        nodes.append(&mut child_nodes);
        edges.append(&mut child_edges);
    }
    (nodes, edges)
}

fn sorted_nodes(mut nodes: Vec<DescribeNode>) -> Vec<DescribeNode> {
    nodes.sort_by(|a, b| a.id.cmp(&b.id));
    nodes
}

fn sorted_edges(edges: Vec<DescribeEdge>) -> Vec<DescribeEdge> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for edge in edges {
        if seen.insert((edge.from.clone(), edge.to.clone())) {
            out.push(edge);
        }
    }
    out.sort_by(|a, b| a.from.cmp(&b.from).then_with(|| a.to.cmp(&b.to)));
    out
}

fn node_json(node: &DescribeNode) -> String {
    let mut fields = vec![
        format!("\"deps\":{}", string_array_json(&node.deps)),
        format!("\"factory\":\"{}\"", json_escape(&node.factory)),
        format!("\"id\":\"{}\"", json_escape(&node.id)),
    ];
    if let Some(meta) = &node.meta {
        let body = meta
            .iter()
            .map(|(k, v)| format!("\"{}\":\"{}\"", json_escape(k), json_escape(v)))
            .collect::<Vec<_>>()
            .join(",");
        fields.push(format!("\"meta\":{{{body}}}"));
    }
    if let Some(name) = &node.name {
        fields.push(format!("\"name\":\"{}\"", json_escape(name)));
    }
    fields.push(format!("\"status\":\"{:?}\"", node.status));
    if let Some(value) = &node.value {
        fields.push(format!("\"value\":{}", value_json(value)));
    }
    format!("{{{}}}", fields.join(","))
}

fn string_array_json(values: &[String]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| format!("\"{}\"", json_escape(value)))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn value_json(value: &DescribeValue) -> String {
    match value {
        DescribeValue::Bool(v) => v.to_string(),
        DescribeValue::I64(v) => v.to_string(),
        DescribeValue::U64(v) => v.to_string(),
        DescribeValue::F64(v) if v.is_finite() => v.to_string(),
        DescribeValue::F64(_) => "\"[non-finite]\"".to_owned(),
        DescribeValue::String(v) => format!("\"{}\"", json_escape(v)),
        DescribeValue::Opaque => "\"[Opaque]\"".to_owned(),
    }
}

fn format_value(node: &DescribeNode) -> String {
    match &node.value {
        None => "<SENTINEL>".to_owned(),
        Some(DescribeValue::Bool(v)) => v.to_string(),
        Some(DescribeValue::I64(v)) => v.to_string(),
        Some(DescribeValue::U64(v)) => v.to_string(),
        Some(DescribeValue::F64(v)) => v.to_string(),
        Some(DescribeValue::String(v)) => format!("\"{}\"", json_escape(v)),
        Some(DescribeValue::Opaque) => "[Opaque]".to_owned(),
    }
}

fn escape_quoted(value: &str) -> String {
    json_escape(value)
}

fn json_escape(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_url_encode(bytes: &[u8]) -> String {
    let mut out = String::new();
    let mut i = 0;
    while i < bytes.len() {
        let a = bytes[i];
        let b = bytes.get(i + 1).copied();
        let c = bytes.get(i + 2).copied();
        out.push(B64[(a >> 2) as usize] as char);
        out.push(B64[(((a & 0x03) << 4) | (b.unwrap_or(0) >> 4)) as usize] as char);
        if let Some(b) = b {
            out.push(B64[(((b & 0x0f) << 2) | (c.unwrap_or(0) >> 6)) as usize] as char);
        }
        if let Some(c) = c {
            out.push(B64[(c & 0x3f) as usize] as char);
        }
        i += 3;
    }
    out.replace('+', "-").replace('/', "_")
}
