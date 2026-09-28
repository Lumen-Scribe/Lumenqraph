//! Semantic diffing of two versions of a contract's **on-chain interface**.
//!
//! Soroban contracts are upgradable in place: the same contract ID can start
//! running new code — and expose a new interface — at any ledger. Because the
//! interface ships *inside* the WASM (see [`crate::spec`]), we can capture it at
//! every upgrade and say precisely what changed: which functions came and went,
//! which signatures moved, which events a consumer can no longer expect.
//!
//! Diffs are computed over the *rendered* signature of each item rather than the
//! raw XDR, because that's the shape a caller actually binds to: a parameter
//! renamed, retyped, or moved from topic to data all change how a client must
//! encode a call or decode an event, and all show up here.
//!
//! ## Severity classification
//!
//! Each change carries a [`Severity`] level so consumers can distinguish
//! genuinely breaking changes from harmless additions:
//!
//! - [`Severity::Breaking`] — removed items, changed parameter types/names,
//!   reordered parameters, changed enum discriminant values, moved event params
//!   from topic to data. Any integration built against the old interface may
//!   stop working.
//! - [`Severity::PotentiallyBreaking`] — added cases to an existing union or
//!   enum (exhaustive decoders / match arms break), added fields to a
//!   map-format event (decoders that reject unknown keys break).
//! - [`Severity::Additive`] — new functions, events, or types added to the
//!   interface. Existing integrations are unaffected.
//!
//! [`SpecDiff::breaking`] is kept for backwards compatibility and remains `true`
//! whenever `max_severity` is [`Severity::Breaking`].
//!
//! A change is [`breaking`](SpecDiff::breaking) if it can invalidate an existing
//! integration: anything removed or changed. Purely additive upgrades are not.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::spec::ContractSpec;

/// Three-level severity classification for a single diff item or an entire
/// [`SpecDiff`].
///
/// The ordering reflects impact: `Additive < PotentiallyBreaking < Breaking`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    /// Only new items were added; no existing consumer is affected.
    Additive,
    /// Existing consumers *may* break depending on how they decode the type
    /// (e.g. exhaustive match on an enum whose case list grew).
    PotentiallyBreaking,
    /// An existing integration built against the old interface may no longer
    /// work (item removed, signature changed, parameters reordered, enum
    /// discriminant changed).
    Breaking,
}

impl Default for Severity {
    fn default() -> Self {
        Severity::Additive
    }
}

/// The difference between two parsed interfaces, oldest to newest.
#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct SpecDiff {
    /// True if anything was removed or changed — i.e. an integration built
    /// against the old interface may no longer work.
    ///
    /// Kept for backwards compatibility. Equivalent to
    /// `max_severity == Severity::Breaking`.
    pub breaking: bool,
    /// The maximum severity across all sections. Use this to drive alert
    /// filtering: `Additive` upgrades are noise-free; `PotentiallyBreaking`
    /// warrants attention for exhaustive decoders; `Breaking` requires action.
    pub max_severity: Severity,
    /// One human-readable line per change, for logs, alerts, and UIs.
    pub summary: Vec<String>,
    pub functions: SectionDiff,
    pub events: SectionDiff,
    /// User-defined types: structs, unions, and enums, keyed by type name.
    pub types: SectionDiff,
}

/// Added / removed / changed items within one section of the interface.
#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct SectionDiff {
    /// Signatures present only in the new interface. Always [`Severity::Additive`].
    pub added: Vec<String>,
    /// Signatures present only in the old interface. Always [`Severity::Breaking`].
    pub removed: Vec<String>,
    /// Items whose name persisted but whose signature moved.
    pub changed: Vec<ChangedItem>,
    /// Items whose name persisted and whose parameter set is identical but
    /// whose parameter *order* changed. Soroban encoding is positional, so a
    /// reorder breaks every caller that passes arguments by position.
    pub reordered: Vec<ReorderedItem>,
    /// Items that gained new cases (union/enum) or fields (map-format event)
    /// without losing or changing existing ones. May break exhaustive decoders.
    pub potentially_breaking: Vec<PotentiallyBreakingItem>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ChangedItem {
    pub name: String,
    pub from: String,
    pub to: String,
    /// Always [`Severity::Breaking`].
    pub severity: Severity,
}

/// A function whose parameter set is identical but whose parameter *order*
/// changed. Because Soroban encoding is positional, reordering parameters is
/// a breaking change: a caller that passes `(from, to, amount)` positionally
/// will send `from` where `amount` is now expected.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReorderedItem {
    pub name: String,
    /// The parameter list as it appeared in the old interface (ordered).
    pub from: String,
    /// The parameter list as it appears in the new interface (ordered).
    pub to: String,
    /// Always [`Severity::Breaking`].
    pub severity: Severity,
}

/// An item that acquired new cases or fields without losing existing ones.
/// Exhaustive decoders (e.g. a Rust `match` with no wildcard arm) may fail on
/// the newly added variant; all other consumers are unaffected.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PotentiallyBreakingItem {
    pub name: String,
    pub from: String,
    pub to: String,
    /// Always [`Severity::PotentiallyBreaking`].
    pub severity: Severity,
    /// A short description of what was added.
    pub description: String,
}

impl SectionDiff {
    fn is_empty(&self) -> bool {
        self.added.is_empty()
            && self.removed.is_empty()
            && self.changed.is_empty()
            && self.reordered.is_empty()
            && self.potentially_breaking.is_empty()
    }

    /// Maximum severity of any item in this section.
    fn max_severity(&self) -> Severity {
        if !self.removed.is_empty() || !self.changed.is_empty() || !self.reordered.is_empty() {
            Severity::Breaking
        } else if !self.potentially_breaking.is_empty() {
            Severity::PotentiallyBreaking
        } else if !self.added.is_empty() {
            Severity::Additive
        } else {
            // Empty section — no severity.
            Severity::Additive
        }
    }

    /// Additions can't break an existing caller; removals, changes, and
    /// reorderings can (Soroban encoding is positional).
    fn has_breaking(&self) -> bool {
        !self.removed.is_empty() || !self.changed.is_empty() || !self.reordered.is_empty()
    }
}

impl SpecDiff {
    /// Diff `old` against `new`. The result reads in the direction of the
    /// upgrade: `added` means "new interface has it, old one didn't".
    pub fn between(old: &ContractSpec, new: &ContractSpec) -> Self {
        let (old_fsigs, old_fparams) = function_sigs(old);
        let (new_fsigs, new_fparams) = function_sigs(new);
        let functions =
            diff_section_with_params(&old_fsigs, &new_fsigs, &old_fparams, &new_fparams);
        let events = diff_events(old, new);
        let types = diff_types(old, new);

        let max_severity = [
            functions.max_severity(),
            events.max_severity(),
            types.max_severity(),
        ]
        .into_iter()
        .max()
        .unwrap_or_default();

        let mut diff = SpecDiff {
            breaking: functions.has_breaking() || events.has_breaking() || types.has_breaking(),
            max_severity,
            summary: Vec::new(),
            functions,
            events,
            types,
        };
        diff.summary = diff.build_summary();
        diff
    }

    /// True when the two interfaces are identical. A contract can be upgraded to
    /// new *code* without changing its interface at all (a bug fix), which is
    /// worth reporting as an upgrade with an empty diff rather than as nothing.
    pub fn is_empty(&self) -> bool {
        self.functions.is_empty() && self.events.is_empty() && self.types.is_empty()
    }

    fn build_summary(&self) -> Vec<String> {
        let mut out = Vec::new();
        for (kind, section) in [
            ("function", &self.functions),
            ("event", &self.events),
            ("type", &self.types),
        ] {
            for sig in &section.removed {
                out.push(format!("removed {kind} {sig}"));
            }
            for item in &section.changed {
                out.push(format!(
                    "changed {kind} {}: {} became {}",
                    item.name, item.from, item.to
                ));
            }
            for item in &section.reordered {
                out.push(format!(
                    "reordered {kind} {}: {} became {}",
                    item.name, item.from, item.to
                ));
            }
            for item in &section.potentially_breaking {
                out.push(format!(
                    "potentially breaking {kind} {}: {}",
                    item.name, item.description
                ));
            }
            for sig in &section.added {
                out.push(format!("added {kind} {sig}"));
            }
        }
        out
    }

    /// The diff as JSON, for storage and API responses.
    pub fn to_json(&self) -> Value {
        json!(self)
    }
}

/// Compare two name-to-signature maps. Names are the identity: a name in both
/// with a different signature is a *change*, not an add plus a remove.
///
/// When both old and new have an entry with the same name but different
/// signatures, we additionally check whether the difference is purely a
/// parameter reorder (same set of `"name: type"` tokens, different order).
/// Reorders are separated into `reordered` rather than `changed` so the
/// summary can label them precisely — but they are still breaking because
/// Soroban encoding is positional.
fn diff_section(old: &BTreeMap<String, String>, new: &BTreeMap<String, String>) -> SectionDiff {
    diff_section_with_params(old, new, &BTreeMap::new(), &BTreeMap::new())
}

/// Like `diff_section` but accepts optional ordered parameter lists per name so
/// that a pure reorder can be distinguished from a type/name change.
/// `old_params` and `new_params` map function name → ordered `["name: type", …]`.
fn diff_section_with_params(
    old: &BTreeMap<String, String>,
    new: &BTreeMap<String, String>,
    old_params: &BTreeMap<String, Vec<String>>,
    new_params: &BTreeMap<String, Vec<String>>,
) -> SectionDiff {
    let names: BTreeSet<&String> = old.keys().chain(new.keys()).collect();
    let mut diff = SectionDiff::default();

    for name in names {
        match (old.get(name), new.get(name)) {
            (Some(before), Some(after)) if before != after => {
                // Check if this is a pure parameter reorder: same set of
                // "name: type" tokens, different order.
                let old_ps = old_params.get(name);
                let new_ps = new_params.get(name);
                if let (Some(op), Some(np)) = (old_ps, new_ps) {
                    if is_param_reorder(op, np) {
                        diff.reordered.push(ReorderedItem {
                            name: name.clone(),
                            from: before.clone(),
                            to: after.clone(),
                            severity: Severity::Breaking,
                        });
                        continue;
                    }
                }
                diff.changed.push(ChangedItem {
                    name: name.clone(),
                    from: before.clone(),
                    to: after.clone(),
                    severity: Severity::Breaking,
                });
            }
            (Some(_), Some(_)) => {}
            (Some(before), None) => diff.removed.push(before.clone()),
            (None, Some(after)) => diff.added.push(after.clone()),
            (None, None) => unreachable!("name came from one of the two maps"),
        }
    }
    diff
}

/// Returns `true` when `old` and `new` contain the same `"name: type"` tokens
/// but in a different order. Both must be non-empty and must differ in order.
fn is_param_reorder(old: &[String], new: &[String]) -> bool {
    if old.len() != new.len() || old == new {
        return false;
    }
    let mut old_sorted = old.to_vec();
    let mut new_sorted = new.to_vec();
    old_sorted.sort();
    new_sorted.sort();
    old_sorted == new_sorted
}

/// Returns a map of function name → rendered signature, plus a companion map
/// of function name → ordered `["param: type", …]` tokens. The second map is
/// used by `diff_section_with_params` to distinguish a pure parameter reorder
/// from a deeper signature change (renamed parameter, changed type, etc.).
fn function_sigs(spec: &ContractSpec) -> (BTreeMap<String, String>, BTreeMap<String, Vec<String>>) {
    let mut sigs = BTreeMap::new();
    let mut params = BTreeMap::new();
    for f in &spec.functions {
        let inputs: Vec<String> = f
            .inputs
            .iter()
            .map(|i| format!("{}: {}", i.name, i.type_name))
            .collect();
        let output = match f.outputs.as_slice() {
            [] => "void".to_string(),
            [one] => one.clone(),
            many => format!("({})", many.join(", ")),
        };
        sigs.insert(
            f.name.clone(),
            format!("{}({}) -> {}", f.name, inputs.join(", "), output),
        );
        params.insert(f.name.clone(), inputs);
    }
    (sigs, params)
}

/// Event signatures carry each param's location and the body's data format:
/// moving a param from topic to data, or switching the body layout, silently
/// breaks every consumer decoding that event, so both belong in the identity.
///
/// Unlike functions, events can have *additive* changes: adding a new param to
/// a map-format event's data block is `PotentiallyBreaking` (not `Breaking`)
/// because a decoder that reads only named fields it knows still works.
fn event_sigs(spec: &ContractSpec) -> BTreeMap<String, String> {
    spec.events
        .iter()
        .map(|e| {
            let params: Vec<String> = e
                .params
                .iter()
                .map(|p| format!("{}: {} @{}", p.name, p.type_name, p.location))
                .collect();
            (
                e.name.clone(),
                format!("{}({}) [{}]", e.name, params.join(", "), e.data_format),
            )
        })
        .collect()
}

/// Diff events with awareness of `PotentiallyBreaking` additions to
/// map-format events.
///
/// If an event exists in both old and new but the signature differs, we check
/// whether only new parameters were *appended* to a `map`-format event body.
/// That case is `PotentiallyBreaking` rather than `Breaking` because consumers
/// reading named fields from the map are still correct; exhaustive decoders
/// that reject unknown keys would fail.
fn diff_events(old: &ContractSpec, new: &ContractSpec) -> SectionDiff {
    let old_sigs = event_sigs(old);
    let new_sigs = event_sigs(new);

    // Build indexed maps of event name → event spec for structured inspection.
    let old_map: BTreeMap<&str, _> = old.events.iter().map(|e| (e.name.as_str(), e)).collect();
    let new_map: BTreeMap<&str, _> = new.events.iter().map(|e| (e.name.as_str(), e)).collect();

    let names: BTreeSet<&String> = old_sigs.keys().chain(new_sigs.keys()).collect();
    let mut diff = SectionDiff::default();

    for name in names {
        match (old_sigs.get(name), new_sigs.get(name)) {
            (Some(before), Some(after)) if before != after => {
                // Check if this is just new params appended to a map-format event.
                if let (Some(oe), Some(ne)) = (old_map.get(name.as_str()), new_map.get(name.as_str())) {
                    if let Some(desc) = is_map_event_additive_extension(oe, ne) {
                        diff.potentially_breaking.push(PotentiallyBreakingItem {
                            name: name.clone(),
                            from: before.clone(),
                            to: after.clone(),
                            severity: Severity::PotentiallyBreaking,
                            description: desc,
                        });
                        continue;
                    }
                }
                diff.changed.push(ChangedItem {
                    name: name.clone(),
                    from: before.clone(),
                    to: after.clone(),
                    severity: Severity::Breaking,
                });
            }
            (Some(_), Some(_)) => {}
            (Some(before), None) => diff.removed.push(before.clone()),
            (None, Some(after)) => diff.added.push(after.clone()),
            (None, None) => unreachable!("name came from one of the two maps"),
        }
    }
    diff
}

/// Returns `Some(description)` if the event change is purely additive (new
/// params appended to a map-format event), `None` if it is a breaking change.
///
/// Map-format events are decoded by field name, so appending new named params
/// does not break consumers that only read the fields they know. However,
/// exhaustive decoders (that reject unknown fields) will fail, so this is
/// `PotentiallyBreaking` rather than `Additive`.
fn is_map_event_additive_extension(
    old: &crate::spec::EventSpec,
    new: &crate::spec::EventSpec,
) -> Option<String> {
    // Only applies to map-format events. data_format is a &'static str: "map".
    if old.data_format != "map" || new.data_format != "map" {
        return None;
    }
    // The old params must be a prefix of the new params (same names, types, locations).
    if new.params.len() <= old.params.len() {
        return None;
    }
    let prefix_matches = old
        .params
        .iter()
        .zip(new.params.iter())
        .all(|(op, np)| op.name == np.name && op.type_name == np.type_name && op.location == np.location);
    if !prefix_matches {
        return None;
    }
    let added_names: Vec<&str> = new.params[old.params.len()..]
        .iter()
        .map(|p| p.name.as_str())
        .collect();
    Some(format!("added map-format event fields: {}", added_names.join(", ")))
}

/// Diff user-defined types (structs, unions, enums) with severity awareness.
///
/// - Added enum/union cases → `PotentiallyBreaking` (exhaustive match arms may
///   need updating; all other consumers are unaffected).
/// - Changed enum discriminant values, removed cases/fields, type changes → `Breaking`.
/// - Entirely new or removed types → `Additive` / `Breaking` respectively.
fn diff_types(old: &ContractSpec, new: &ContractSpec) -> SectionDiff {
    let old_sigs = type_sigs(old);
    let new_sigs = type_sigs(new);

    let names: BTreeSet<&String> = old_sigs.keys().chain(new_sigs.keys()).collect();
    let mut diff = SectionDiff::default();

    // Build indexed maps for structured inspection.
    let old_enums: BTreeMap<&str, _> = old.enums.iter().map(|e| (e.name.as_str(), e)).collect();
    let new_enums: BTreeMap<&str, _> = new.enums.iter().map(|e| (e.name.as_str(), e)).collect();
    let old_unions: BTreeMap<&str, _> = old.unions.iter().map(|u| (u.name.as_str(), u)).collect();
    let new_unions: BTreeMap<&str, _> = new.unions.iter().map(|u| (u.name.as_str(), u)).collect();

    for name in names {
        match (old_sigs.get(name), new_sigs.get(name)) {
            (Some(before), Some(after)) if before != after => {
                // Check if this is an additive enum case extension.
                if let (Some(oe), Some(ne)) = (old_enums.get(name.as_str()), new_enums.get(name.as_str())) {
                    if let Some(desc) = is_enum_additive_extension(oe, ne) {
                        diff.potentially_breaking.push(PotentiallyBreakingItem {
                            name: name.clone(),
                            from: before.clone(),
                            to: after.clone(),
                            severity: Severity::PotentiallyBreaking,
                            description: desc,
                        });
                        continue;
                    }
                }
                // Check if this is an additive union case extension.
                if let (Some(ou), Some(nu)) = (old_unions.get(name.as_str()), new_unions.get(name.as_str())) {
                    if let Some(desc) = is_union_additive_extension(ou, nu) {
                        diff.potentially_breaking.push(PotentiallyBreakingItem {
                            name: name.clone(),
                            from: before.clone(),
                            to: after.clone(),
                            severity: Severity::PotentiallyBreaking,
                            description: desc,
                        });
                        continue;
                    }
                }
                diff.changed.push(ChangedItem {
                    name: name.clone(),
                    from: before.clone(),
                    to: after.clone(),
                    severity: Severity::Breaking,
                });
            }
            (Some(_), Some(_)) => {}
            (Some(before), None) => diff.removed.push(before.clone()),
            (None, Some(after)) => diff.added.push(after.clone()),
            (None, None) => unreachable!("name came from one of the two maps"),
        }
    }
    diff
}

/// Returns `Some(description)` when an enum changed only by adding new cases
/// (without removing or changing any existing case name or value).
fn is_enum_additive_extension(
    old: &crate::spec::UdtEnum,
    new: &crate::spec::UdtEnum,
) -> Option<String> {
    if new.cases.len() <= old.cases.len() {
        return None;
    }
    // All old cases must be present in new with the same value.
    let new_case_map: BTreeMap<&str, u32> =
        new.cases.iter().map(|(n, v)| (n.as_str(), *v)).collect();
    let all_preserved = old
        .cases
        .iter()
        .all(|(name, val)| new_case_map.get(name.as_str()) == Some(val));
    if !all_preserved {
        return None;
    }
    let added_names: Vec<&str> = new
        .cases
        .iter()
        .filter(|(n, _)| !old.cases.iter().any(|(on, _)| on == n))
        .map(|(n, _)| n.as_str())
        .collect();
    Some(format!("added enum cases: {}", added_names.join(", ")))
}

/// Returns `Some(description)` when a union changed only by appending new cases
/// (without removing or changing any existing case name or types).
fn is_union_additive_extension(
    old: &crate::spec::UdtUnion,
    new: &crate::spec::UdtUnion,
) -> Option<String> {
    if new.cases.len() <= old.cases.len() {
        return None;
    }
    // All old cases must be present in new at the same position with same names
    // and same type signatures (compare by rendered type_names, not raw XDR).
    let all_preserved = old.cases.iter().zip(new.cases.iter()).all(|(oc, nc)| {
        oc.name == nc.name && oc.type_names == nc.type_names
    });
    if !all_preserved {
        return None;
    }
    let added_names: Vec<&str> = new.cases[old.cases.len()..]
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    Some(format!("added union cases: {}", added_names.join(", ")))
}

/// Structs, unions, and enums share one namespace, so they share one section —
/// which also means a type that changes kind reads as a change, not a swap.
fn type_sigs(spec: &ContractSpec) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();

    for s in &spec.structs {
        let fields: Vec<String> = s
            .fields
            .iter()
            .map(|f| format!("{}: {}", f.name, f.type_name))
            .collect();
        out.insert(
            s.name.clone(),
            format!("struct {} {{ {} }}", s.name, fields.join(", ")),
        );
    }
    for u in &spec.unions {
        let cases: Vec<String> = u
            .cases
            .iter()
            .map(|c| {
                if c.type_names.is_empty() {
                    c.name.clone()
                } else {
                    format!("{}({})", c.name, c.type_names.join(", "))
                }
            })
            .collect();
        out.insert(
            u.name.clone(),
            format!("union {} {{ {} }}", u.name, cases.join(", ")),
        );
    }
    for e in &spec.enums {
        let cases: Vec<String> = e
            .cases
            .iter()
            .map(|(name, value)| format!("{name} = {value}"))
            .collect();
        out.insert(
            e.name.clone(),
            format!("enum {} {{ {} }}", e.name, cases.join(", ")),
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use stellar_xdr::curr::{
        Limits, ScSpecEntry, ScSpecEventDataFormat, ScSpecEventParamLocationV0, ScSpecEventParamV0,
        ScSpecEventV0, ScSpecFunctionInputV0, ScSpecFunctionV0, ScSpecTypeDef, ScSymbol, WriteXdr,
    };

    fn spec_of(entries: &[ScSpecEntry]) -> ContractSpec {
        let mut body = Vec::new();
        for e in entries {
            body.extend(e.to_xdr(Limits::none()).unwrap());
        }
        ContractSpec::from_spec_xdr(&body).expect("test spec should parse")
    }

    /// `name(<inputs>) -> <output>`
    fn func(
        name: &str,
        inputs: &[(&str, ScSpecTypeDef)],
        output: Option<ScSpecTypeDef>,
    ) -> ScSpecEntry {
        ScSpecEntry::FunctionV0(ScSpecFunctionV0 {
            doc: "".try_into().unwrap(),
            name: ScSymbol(name.try_into().unwrap()),
            inputs: inputs
                .iter()
                .map(|(n, t)| ScSpecFunctionInputV0 {
                    doc: "".try_into().unwrap(),
                    name: (*n).try_into().unwrap(),
                    type_: t.clone(),
                })
                .collect::<Vec<_>>()
                .try_into()
                .unwrap(),
            outputs: output.into_iter().collect::<Vec<_>>().try_into().unwrap(),
        })
    }

    fn event(
        name: &str,
        params: &[(&str, ScSpecTypeDef, ScSpecEventParamLocationV0)],
    ) -> ScSpecEntry {
        ScSpecEntry::EventV0(ScSpecEventV0 {
            doc: "".try_into().unwrap(),
            lib: "".try_into().unwrap(),
            name: ScSymbol(name.try_into().unwrap()),
            prefix_topics: vec![ScSymbol(name.try_into().unwrap())].try_into().unwrap(),
            params: params
                .iter()
                .map(|(n, t, loc)| ScSpecEventParamV0 {
                    doc: "".try_into().unwrap(),
                    name: (*n).try_into().unwrap(),
                    type_: t.clone(),
                    location: *loc,
                })
                .collect::<Vec<_>>()
                .try_into()
                .unwrap(),
            data_format: ScSpecEventDataFormat::SingleValue,
        })
    }

    fn map_event(
        name: &str,
        params: &[(&str, ScSpecTypeDef, ScSpecEventParamLocationV0)],
    ) -> ScSpecEntry {
        ScSpecEntry::EventV0(ScSpecEventV0 {
            doc: "".try_into().unwrap(),
            lib: "".try_into().unwrap(),
            name: ScSymbol(name.try_into().unwrap()),
            prefix_topics: vec![ScSymbol(name.try_into().unwrap())].try_into().unwrap(),
            params: params
                .iter()
                .map(|(n, t, loc)| ScSpecEventParamV0 {
                    doc: "".try_into().unwrap(),
                    name: (*n).try_into().unwrap(),
                    type_: t.clone(),
                    location: *loc,
                })
                .collect::<Vec<_>>()
                .try_into()
                .unwrap(),
            data_format: ScSpecEventDataFormat::Map,
        })
    }

    #[test]
    fn identical_interfaces_produce_an_empty_non_breaking_diff() {
        let a = spec_of(&[func(
            "balance",
            &[("id", ScSpecTypeDef::Address)],
            Some(ScSpecTypeDef::I128),
        )]);
        let b = spec_of(&[func(
            "balance",
            &[("id", ScSpecTypeDef::Address)],
            Some(ScSpecTypeDef::I128),
        )]);
        let d = SpecDiff::between(&a, &b);
        assert!(d.is_empty());
        assert!(!d.breaking);
        assert_eq!(d.max_severity, Severity::Additive);
        assert!(d.summary.is_empty());
    }

    #[test]
    fn an_added_function_is_not_breaking() {
        let old = spec_of(&[func("balance", &[], Some(ScSpecTypeDef::I128))]);
        let new = spec_of(&[
            func("balance", &[], Some(ScSpecTypeDef::I128)),
            func("pause", &[], None),
        ]);
        let d = SpecDiff::between(&old, &new);
        assert!(!d.breaking);
        assert_eq!(d.max_severity, Severity::Additive);
        assert_eq!(d.functions.added, vec!["pause() -> void"]);
        assert_eq!(d.summary, vec!["added function pause() -> void"]);
    }

    #[test]
    fn a_removed_function_is_breaking() {
        let old = spec_of(&[
            func("balance", &[], Some(ScSpecTypeDef::I128)),
            func("withdraw", &[("amount", ScSpecTypeDef::I128)], None),
        ]);
        let new = spec_of(&[func("balance", &[], Some(ScSpecTypeDef::I128))]);
        let d = SpecDiff::between(&old, &new);
        assert!(d.breaking);
        assert_eq!(d.max_severity, Severity::Breaking);
        assert_eq!(d.functions.removed, vec!["withdraw(amount: i128) -> void"]);
    }

    #[test]
    fn a_retyped_parameter_is_a_change_not_an_add_and_remove() {
        let old = spec_of(&[func("mint", &[("amount", ScSpecTypeDef::I128)], None)]);
        let new = spec_of(&[func("mint", &[("amount", ScSpecTypeDef::U128)], None)]);
        let d = SpecDiff::between(&old, &new);
        assert!(d.breaking);
        assert_eq!(d.max_severity, Severity::Breaking);
        assert!(d.functions.added.is_empty());
        assert!(d.functions.removed.is_empty());
        assert_eq!(
            d.functions.changed,
            vec![ChangedItem {
                name: "mint".into(),
                from: "mint(amount: i128) -> void".into(),
                to: "mint(amount: u128) -> void".into(),
                severity: Severity::Breaking,
            }]
        );
    }

    #[test]
    fn a_changed_return_type_is_breaking() {
        let old = spec_of(&[func("decimals", &[], Some(ScSpecTypeDef::U32))]);
        let new = spec_of(&[func("decimals", &[], Some(ScSpecTypeDef::U64))]);
        let d = SpecDiff::between(&old, &new);
        assert!(d.breaking);
        assert_eq!(d.max_severity, Severity::Breaking);
        assert_eq!(d.functions.changed[0].to, "decimals() -> u64");
    }

    #[test]
    fn moving_an_event_param_from_topic_to_data_is_breaking() {
        // Same name, same type, same order — only the location moved. A consumer
        // reading `to` out of the topic list silently gets nothing.
        let old = spec_of(&[event(
            "transfer",
            &[
                (
                    "from",
                    ScSpecTypeDef::Address,
                    ScSpecEventParamLocationV0::TopicList,
                ),
                (
                    "to",
                    ScSpecTypeDef::Address,
                    ScSpecEventParamLocationV0::TopicList,
                ),
            ],
        )]);
        let new = spec_of(&[event(
            "transfer",
            &[
                (
                    "from",
                    ScSpecTypeDef::Address,
                    ScSpecEventParamLocationV0::TopicList,
                ),
                (
                    "to",
                    ScSpecTypeDef::Address,
                    ScSpecEventParamLocationV0::Data,
                ),
            ],
        )]);
        let d = SpecDiff::between(&old, &new);
        assert!(d.breaking);
        assert_eq!(d.max_severity, Severity::Breaking);
        assert_eq!(d.events.changed.len(), 1);
        assert!(d.events.changed[0].to.contains("to: Address @data"));
    }

    #[test]
    fn a_removed_event_is_breaking() {
        let old = spec_of(&[event(
            "burn",
            &[(
                "amount",
                ScSpecTypeDef::I128,
                ScSpecEventParamLocationV0::Data,
            )],
        )]);
        let new = spec_of(&[func("balance", &[], Some(ScSpecTypeDef::I128))]);
        let d = SpecDiff::between(&old, &new);
        assert!(d.breaking);
        assert_eq!(d.max_severity, Severity::Breaking);
        assert_eq!(d.events.removed.len(), 1);
        assert!(d.events.removed[0].starts_with("burn(amount: i128 @data)"));
    }

    /// Summary lines are grouped by section (functions, then events, then
    /// types) and ordered removed-changed-added within each, so the most
    /// disruptive lines of each section lead.
    #[test]
    fn summary_reports_every_section() {
        let old = spec_of(&[
            func("withdraw", &[], None),
            event(
                "burn",
                &[(
                    "amount",
                    ScSpecTypeDef::I128,
                    ScSpecEventParamLocationV0::Data,
                )],
            ),
        ]);
        let new = spec_of(&[func("pause", &[], None)]);
        let d = SpecDiff::between(&old, &new);
        assert!(d.breaking);
        assert_eq!(
            d.summary,
            vec![
                "removed function withdraw() -> void",
                "added function pause() -> void",
                "removed event burn(amount: i128 @data) [single]",
            ]
        );
    }

    #[test]
    fn an_added_struct_is_not_breaking() {
        use stellar_xdr::curr::{ScSpecUdtStructFieldV0, ScSpecUdtStructV0};

        let old = spec_of(&[func("balance", &[], Some(ScSpecTypeDef::I128))]);
        let position_struct = ScSpecEntry::UdtStructV0(ScSpecUdtStructV0 {
            doc: "".try_into().unwrap(),
            lib: "".try_into().unwrap(),
            name: "Position".try_into().unwrap(),
            fields: vec![
                ScSpecUdtStructFieldV0 {
                    doc: "".try_into().unwrap(),
                    name: "borrower".try_into().unwrap(),
                    type_: ScSpecTypeDef::Address,
                },
                ScSpecUdtStructFieldV0 {
                    doc: "".try_into().unwrap(),
                    name: "amount".try_into().unwrap(),
                    type_: ScSpecTypeDef::I128,
                },
            ]
            .try_into()
            .unwrap(),
        });
        let new = spec_of(&[
            func("balance", &[], Some(ScSpecTypeDef::I128)),
            position_struct,
        ]);
        let d = SpecDiff::between(&old, &new);
        assert!(!d.breaking);
        assert_eq!(d.max_severity, Severity::Additive);
        assert_eq!(d.types.added.len(), 1);
        assert!(d.types.added[0].contains("struct Position"));
    }

    #[test]
    fn a_removed_struct_is_breaking() {
        use stellar_xdr::curr::{ScSpecUdtStructFieldV0, ScSpecUdtStructV0};

        let position_struct = ScSpecEntry::UdtStructV0(ScSpecUdtStructV0 {
            doc: "".try_into().unwrap(),
            lib: "".try_into().unwrap(),
            name: "Position".try_into().unwrap(),
            fields: vec![ScSpecUdtStructFieldV0 {
                doc: "".try_into().unwrap(),
                name: "borrower".try_into().unwrap(),
                type_: ScSpecTypeDef::Address,
            }]
            .try_into()
            .unwrap(),
        });
        let old = spec_of(&[
            func("balance", &[], Some(ScSpecTypeDef::I128)),
            position_struct,
        ]);
        let new = spec_of(&[func("balance", &[], Some(ScSpecTypeDef::I128))]);
        let d = SpecDiff::between(&old, &new);
        assert!(d.breaking);
        assert_eq!(d.max_severity, Severity::Breaking);
        assert_eq!(d.types.removed.len(), 1);
    }

    #[test]
    fn a_changed_struct_field_type_is_breaking() {
        use stellar_xdr::curr::{ScSpecUdtStructFieldV0, ScSpecUdtStructV0};

        let position_old = ScSpecEntry::UdtStructV0(ScSpecUdtStructV0 {
            doc: "".try_into().unwrap(),
            lib: "".try_into().unwrap(),
            name: "Position".try_into().unwrap(),
            fields: vec![ScSpecUdtStructFieldV0 {
                doc: "".try_into().unwrap(),
                name: "amount".try_into().unwrap(),
                type_: ScSpecTypeDef::I128,
            }]
            .try_into()
            .unwrap(),
        });

        let position_new = ScSpecEntry::UdtStructV0(ScSpecUdtStructV0 {
            doc: "".try_into().unwrap(),
            lib: "".try_into().unwrap(),
            name: "Position".try_into().unwrap(),
            fields: vec![ScSpecUdtStructFieldV0 {
                doc: "".try_into().unwrap(),
                name: "amount".try_into().unwrap(),
                type_: ScSpecTypeDef::U128,
            }]
            .try_into()
            .unwrap(),
        });

        let old = spec_of(&[position_old]);
        let new = spec_of(&[position_new]);
        let d = SpecDiff::between(&old, &new);
        assert!(d.breaking);
        assert_eq!(d.max_severity, Severity::Breaking);
        assert_eq!(d.types.changed.len(), 1);
        assert!(d.types.changed[0].from.contains("i128"));
        assert!(d.types.changed[0].to.contains("u128"));
    }

    #[test]
    fn an_added_enum_is_not_breaking() {
        use stellar_xdr::curr::{ScSpecUdtEnumCaseV0, ScSpecUdtEnumV0};

        let old = spec_of(&[func("balance", &[], Some(ScSpecTypeDef::I128))]);
        let status_enum = ScSpecEntry::UdtEnumV0(ScSpecUdtEnumV0 {
            doc: "".try_into().unwrap(),
            lib: "".try_into().unwrap(),
            name: "Status".try_into().unwrap(),
            cases: vec![
                ScSpecUdtEnumCaseV0 {
                    doc: "".try_into().unwrap(),
                    name: "Active".try_into().unwrap(),
                    value: 0,
                },
                ScSpecUdtEnumCaseV0 {
                    doc: "".try_into().unwrap(),
                    name: "Inactive".try_into().unwrap(),
                    value: 1,
                },
            ]
            .try_into()
            .unwrap(),
        });
        let new = spec_of(&[func("balance", &[], Some(ScSpecTypeDef::I128)), status_enum]);
        let d = SpecDiff::between(&old, &new);
        assert!(!d.breaking);
        assert_eq!(d.max_severity, Severity::Additive);
        assert_eq!(d.types.added.len(), 1);
        assert!(d.types.added[0].contains("enum Status"));
    }

    /// Adding a new case to an existing enum is potentially breaking (exhaustive
    /// decoders will fail) but not fully breaking (non-exhaustive decoders are fine).
    #[test]
    fn adding_an_enum_case_is_potentially_breaking_not_breaking() {
        use stellar_xdr::curr::{ScSpecUdtEnumCaseV0, ScSpecUdtEnumV0};

        let status_old = ScSpecEntry::UdtEnumV0(ScSpecUdtEnumV0 {
            doc: "".try_into().unwrap(),
            lib: "".try_into().unwrap(),
            name: "Status".try_into().unwrap(),
            cases: vec![
                ScSpecUdtEnumCaseV0 {
                    doc: "".try_into().unwrap(),
                    name: "Active".try_into().unwrap(),
                    value: 0,
                },
                ScSpecUdtEnumCaseV0 {
                    doc: "".try_into().unwrap(),
                    name: "Inactive".try_into().unwrap(),
                    value: 1,
                },
            ]
            .try_into()
            .unwrap(),
        });

        let status_new = ScSpecEntry::UdtEnumV0(ScSpecUdtEnumV0 {
            doc: "".try_into().unwrap(),
            lib: "".try_into().unwrap(),
            name: "Status".try_into().unwrap(),
            cases: vec![
                ScSpecUdtEnumCaseV0 {
                    doc: "".try_into().unwrap(),
                    name: "Active".try_into().unwrap(),
                    value: 0,
                },
                ScSpecUdtEnumCaseV0 {
                    doc: "".try_into().unwrap(),
                    name: "Inactive".try_into().unwrap(),
                    value: 1,
                },
                ScSpecUdtEnumCaseV0 {
                    doc: "".try_into().unwrap(),
                    name: "Pending".try_into().unwrap(),
                    value: 2,
                },
            ]
            .try_into()
            .unwrap(),
        });

        let old = spec_of(&[status_old]);
        let new = spec_of(&[status_new]);
        let d = SpecDiff::between(&old, &new);
        // Not breaking — no existing case was removed or changed.
        assert!(!d.breaking, "adding an enum case must not be breaking");
        assert_eq!(
            d.max_severity,
            Severity::PotentiallyBreaking,
            "adding an enum case should be potentially_breaking"
        );
        assert!(d.types.changed.is_empty(), "must not appear in 'changed'");
        assert_eq!(
            d.types.potentially_breaking.len(),
            1,
            "must appear in 'potentially_breaking'"
        );
        assert!(
            d.types.potentially_breaking[0].description.contains("Pending"),
            "description should name the new case: {}",
            d.types.potentially_breaking[0].description
        );
    }

    /// Changing an enum case's discriminant value is fully breaking — any
    /// serialised value that matched the old case now maps to a different case.
    #[test]
    fn changing_enum_case_value_is_breaking() {
        use stellar_xdr::curr::{ScSpecUdtEnumCaseV0, ScSpecUdtEnumV0};

        let status_old = ScSpecEntry::UdtEnumV0(ScSpecUdtEnumV0 {
            doc: "".try_into().unwrap(),
            lib: "".try_into().unwrap(),
            name: "Status".try_into().unwrap(),
            cases: vec![
                ScSpecUdtEnumCaseV0 {
                    doc: "".try_into().unwrap(),
                    name: "Active".try_into().unwrap(),
                    value: 0,
                },
                ScSpecUdtEnumCaseV0 {
                    doc: "".try_into().unwrap(),
                    name: "Inactive".try_into().unwrap(),
                    value: 1,
                },
            ]
            .try_into()
            .unwrap(),
        });

        // Inactive's value changed from 1 → 2.
        let status_new = ScSpecEntry::UdtEnumV0(ScSpecUdtEnumV0 {
            doc: "".try_into().unwrap(),
            lib: "".try_into().unwrap(),
            name: "Status".try_into().unwrap(),
            cases: vec![
                ScSpecUdtEnumCaseV0 {
                    doc: "".try_into().unwrap(),
                    name: "Active".try_into().unwrap(),
                    value: 0,
                },
                ScSpecUdtEnumCaseV0 {
                    doc: "".try_into().unwrap(),
                    name: "Inactive".try_into().unwrap(),
                    value: 2,
                },
            ]
            .try_into()
            .unwrap(),
        });

        let old = spec_of(&[status_old]);
        let new = spec_of(&[status_new]);
        let d = SpecDiff::between(&old, &new);
        assert!(d.breaking, "changing an enum case value must be breaking");
        assert_eq!(d.max_severity, Severity::Breaking);
        assert_eq!(
            d.types.changed.len(),
            1,
            "must appear in 'changed', not 'potentially_breaking'"
        );
        assert!(d.types.potentially_breaking.is_empty());
    }

    #[test]
    fn a_removed_enum_case_is_breaking() {
        use stellar_xdr::curr::{ScSpecUdtEnumCaseV0, ScSpecUdtEnumV0};

        let status_old = ScSpecEntry::UdtEnumV0(ScSpecUdtEnumV0 {
            doc: "".try_into().unwrap(),
            lib: "".try_into().unwrap(),
            name: "Status".try_into().unwrap(),
            cases: vec![
                ScSpecUdtEnumCaseV0 {
                    doc: "".try_into().unwrap(),
                    name: "Active".try_into().unwrap(),
                    value: 0,
                },
                ScSpecUdtEnumCaseV0 {
                    doc: "".try_into().unwrap(),
                    name: "Inactive".try_into().unwrap(),
                    value: 1,
                },
            ]
            .try_into()
            .unwrap(),
        });

        let status_new = ScSpecEntry::UdtEnumV0(ScSpecUdtEnumV0 {
            doc: "".try_into().unwrap(),
            lib: "".try_into().unwrap(),
            name: "Status".try_into().unwrap(),
            cases: vec![ScSpecUdtEnumCaseV0 {
                doc: "".try_into().unwrap(),
                name: "Active".try_into().unwrap(),
                value: 0,
            }]
            .try_into()
            .unwrap(),
        });

        let old = spec_of(&[status_old]);
        let new = spec_of(&[status_new]);
        let d = SpecDiff::between(&old, &new);
        assert!(d.breaking);
        assert_eq!(d.max_severity, Severity::Breaking);
        assert_eq!(d.types.changed.len(), 1);
    }

    /// Adding a new void case to an existing union is potentially breaking:
    /// exhaustive match arms fail, but non-exhaustive decoders are fine.
    #[test]
    fn adding_a_union_case_is_potentially_breaking_not_breaking() {
        use stellar_xdr::curr::{ScSpecUdtUnionCaseV0, ScSpecUdtUnionCaseVoidV0, ScSpecUdtUnionV0};

        let action_old = ScSpecEntry::UdtUnionV0(ScSpecUdtUnionV0 {
            doc: "".try_into().unwrap(),
            lib: "".try_into().unwrap(),
            name: "Action".try_into().unwrap(),
            cases: vec![
                ScSpecUdtUnionCaseV0::VoidV0(ScSpecUdtUnionCaseVoidV0 {
                    doc: "".try_into().unwrap(),
                    name: "Cancel".try_into().unwrap(),
                }),
                ScSpecUdtUnionCaseV0::VoidV0(ScSpecUdtUnionCaseVoidV0 {
                    doc: "".try_into().unwrap(),
                    name: "Execute".try_into().unwrap(),
                }),
            ]
            .try_into()
            .unwrap(),
        });

        let action_new = ScSpecEntry::UdtUnionV0(ScSpecUdtUnionV0 {
            doc: "".try_into().unwrap(),
            lib: "".try_into().unwrap(),
            name: "Action".try_into().unwrap(),
            cases: vec![
                ScSpecUdtUnionCaseV0::VoidV0(ScSpecUdtUnionCaseVoidV0 {
                    doc: "".try_into().unwrap(),
                    name: "Cancel".try_into().unwrap(),
                }),
                ScSpecUdtUnionCaseV0::VoidV0(ScSpecUdtUnionCaseVoidV0 {
                    doc: "".try_into().unwrap(),
                    name: "Execute".try_into().unwrap(),
                }),
                ScSpecUdtUnionCaseV0::VoidV0(ScSpecUdtUnionCaseVoidV0 {
                    doc: "".try_into().unwrap(),
                    name: "Pause".try_into().unwrap(),
                }),
            ]
            .try_into()
            .unwrap(),
        });

        let old = spec_of(&[action_old]);
        let new = spec_of(&[action_new]);
        let d = SpecDiff::between(&old, &new);
        assert!(!d.breaking, "adding a union case must not be breaking");
        assert_eq!(d.max_severity, Severity::PotentiallyBreaking);
        assert!(d.types.changed.is_empty(), "must not appear in 'changed'");
        assert_eq!(d.types.potentially_breaking.len(), 1);
        assert!(
            d.types.potentially_breaking[0].description.contains("Pause"),
            "description should name the new case"
        );
    }

    #[test]
    fn an_added_union_is_not_breaking() {
        use stellar_xdr::curr::{ScSpecUdtUnionCaseV0, ScSpecUdtUnionCaseVoidV0, ScSpecUdtUnionV0};

        let old = spec_of(&[func("balance", &[], Some(ScSpecTypeDef::I128))]);
        let action_union = ScSpecEntry::UdtUnionV0(ScSpecUdtUnionV0 {
            doc: "".try_into().unwrap(),
            lib: "".try_into().unwrap(),
            name: "Action".try_into().unwrap(),
            cases: vec![
                ScSpecUdtUnionCaseV0::VoidV0(ScSpecUdtUnionCaseVoidV0 {
                    doc: "".try_into().unwrap(),
                    name: "Cancel".try_into().unwrap(),
                }),
                ScSpecUdtUnionCaseV0::VoidV0(ScSpecUdtUnionCaseVoidV0 {
                    doc: "".try_into().unwrap(),
                    name: "Execute".try_into().unwrap(),
                }),
            ]
            .try_into()
            .unwrap(),
        });
        let new = spec_of(&[
            func("balance", &[], Some(ScSpecTypeDef::I128)),
            action_union,
        ]);
        let d = SpecDiff::between(&old, &new);
        assert!(!d.breaking);
        assert_eq!(d.max_severity, Severity::Additive);
        assert_eq!(d.types.added.len(), 1);
        assert!(d.types.added[0].contains("union Action"));
    }

    #[test]
    fn a_removed_union_case_is_breaking() {
        use stellar_xdr::curr::{ScSpecUdtUnionCaseV0, ScSpecUdtUnionCaseVoidV0, ScSpecUdtUnionV0};

        let action_old = ScSpecEntry::UdtUnionV0(ScSpecUdtUnionV0 {
            doc: "".try_into().unwrap(),
            lib: "".try_into().unwrap(),
            name: "Action".try_into().unwrap(),
            cases: vec![
                ScSpecUdtUnionCaseV0::VoidV0(ScSpecUdtUnionCaseVoidV0 {
                    doc: "".try_into().unwrap(),
                    name: "Cancel".try_into().unwrap(),
                }),
                ScSpecUdtUnionCaseV0::VoidV0(ScSpecUdtUnionCaseVoidV0 {
                    doc: "".try_into().unwrap(),
                    name: "Execute".try_into().unwrap(),
                }),
            ]
            .try_into()
            .unwrap(),
        });

        let action_new = ScSpecEntry::UdtUnionV0(ScSpecUdtUnionV0 {
            doc: "".try_into().unwrap(),
            lib: "".try_into().unwrap(),
            name: "Action".try_into().unwrap(),
            cases: vec![ScSpecUdtUnionCaseV0::VoidV0(ScSpecUdtUnionCaseVoidV0 {
                doc: "".try_into().unwrap(),
                name: "Cancel".try_into().unwrap(),
            })]
            .try_into()
            .unwrap(),
        });

        let old = spec_of(&[action_old]);
        let new = spec_of(&[action_new]);
        let d = SpecDiff::between(&old, &new);
        assert!(d.breaking);
        assert_eq!(d.max_severity, Severity::Breaking);
        assert_eq!(d.types.changed.len(), 1);
    }

    #[test]
    fn a_renamed_function_shows_as_removed_and_added() {
        let old = spec_of(&[func("withdraw", &[("amount", ScSpecTypeDef::I128)], None)]);
        let new = spec_of(&[func("pull", &[("amount", ScSpecTypeDef::I128)], None)]);
        let d = SpecDiff::between(&old, &new);
        assert!(d.breaking);
        assert_eq!(d.max_severity, Severity::Breaking);
        assert_eq!(d.functions.added.len(), 1);
        assert_eq!(d.functions.removed.len(), 1);
        assert!(d.functions.changed.is_empty());
    }

    /// Reordering parameters is a breaking change because Soroban encoding is
    /// positional: a caller that passes `(from, to, amount)` by position will
    /// send `from` where `amount` is now expected. The diff must flag this
    /// explicitly as a `reordered` item rather than a generic `changed` item,
    /// and the `breaking` flag must be set.
    #[test]
    fn reordered_parameters_are_breaking_and_flagged_as_reordered() {
        let old = spec_of(&[func(
            "transfer",
            &[
                ("from", ScSpecTypeDef::Address),
                ("to", ScSpecTypeDef::Address),
                ("amount", ScSpecTypeDef::I128),
            ],
            None,
        )]);
        let new = spec_of(&[func(
            "transfer",
            &[
                ("amount", ScSpecTypeDef::I128),
                ("from", ScSpecTypeDef::Address),
                ("to", ScSpecTypeDef::Address),
            ],
            None,
        )]);
        let d = SpecDiff::between(&old, &new);
        assert!(d.breaking, "parameter reorder must be breaking");
        assert_eq!(d.max_severity, Severity::Breaking);
        assert!(
            d.functions.changed.is_empty(),
            "a pure reorder must not appear in 'changed'"
        );
        assert_eq!(
            d.functions.reordered.len(),
            1,
            "a pure reorder must appear in 'reordered'"
        );
        let item = &d.functions.reordered[0];
        assert_eq!(item.name, "transfer");
        assert!(
            item.from.contains("from: Address"),
            "from-signature should reference old order"
        );
        assert!(
            item.to.contains("amount: i128"),
            "to-signature should reference new order"
        );
        // The summary line should say "reordered function …"
        assert_eq!(d.summary.len(), 1);
        assert!(
            d.summary[0].starts_with("reordered function transfer"),
            "summary line should start with 'reordered function transfer', got: {}",
            d.summary[0]
        );
    }

    /// A change that renames or retypes a parameter is NOT a pure reorder —
    /// it must still appear in `changed`, not `reordered`.
    #[test]
    fn renamed_parameter_is_changed_not_reordered() {
        // Same types, different names — not a reorder.
        let old = spec_of(&[func(
            "transfer",
            &[
                ("from", ScSpecTypeDef::Address),
                ("to", ScSpecTypeDef::Address),
            ],
            None,
        )]);
        let new = spec_of(&[func(
            "transfer",
            &[
                ("sender", ScSpecTypeDef::Address),
                ("recipient", ScSpecTypeDef::Address),
            ],
            None,
        )]);
        let d = SpecDiff::between(&old, &new);
        assert!(d.breaking);
        assert_eq!(d.functions.changed.len(), 1, "renamed params → changed");
        assert!(
            d.functions.reordered.is_empty(),
            "renamed params → not reordered"
        );
    }

    #[test]
    fn an_added_event_is_not_breaking() {
        let old = spec_of(&[func("balance", &[], Some(ScSpecTypeDef::I128))]);
        let new = spec_of(&[
            func("balance", &[], Some(ScSpecTypeDef::I128)),
            event(
                "mint",
                &[(
                    "amount",
                    ScSpecTypeDef::I128,
                    ScSpecEventParamLocationV0::Data,
                )],
            ),
        ]);
        let d = SpecDiff::between(&old, &new);
        assert!(!d.breaking);
        assert_eq!(d.max_severity, Severity::Additive);
        assert_eq!(d.events.added.len(), 1);
    }

    /// Adding a field to a map-format event is potentially breaking (exhaustive
    /// decoders that reject unknown fields will fail) but not fully breaking.
    #[test]
    fn adding_map_event_field_is_potentially_breaking() {
        let old = spec_of(&[map_event(
            "swap",
            &[
                (
                    "from",
                    ScSpecTypeDef::Address,
                    ScSpecEventParamLocationV0::TopicList,
                ),
                (
                    "amount_in",
                    ScSpecTypeDef::I128,
                    ScSpecEventParamLocationV0::Data,
                ),
            ],
        )]);
        let new = spec_of(&[map_event(
            "swap",
            &[
                (
                    "from",
                    ScSpecTypeDef::Address,
                    ScSpecEventParamLocationV0::TopicList,
                ),
                (
                    "amount_in",
                    ScSpecTypeDef::I128,
                    ScSpecEventParamLocationV0::Data,
                ),
                (
                    "amount_out",
                    ScSpecTypeDef::I128,
                    ScSpecEventParamLocationV0::Data,
                ),
            ],
        )]);
        let d = SpecDiff::between(&old, &new);
        assert!(
            !d.breaking,
            "adding a map-event field must not be fully breaking"
        );
        assert_eq!(d.max_severity, Severity::PotentiallyBreaking);
        assert!(d.events.changed.is_empty(), "must not be in 'changed'");
        assert_eq!(d.events.potentially_breaking.len(), 1);
        assert!(
            d.events.potentially_breaking[0]
                .description
                .contains("amount_out"),
            "description should name the new field"
        );
    }

    #[test]
    fn multiple_changes_are_all_tracked() {
        use stellar_xdr::curr::{ScSpecUdtStructFieldV0, ScSpecUdtStructV0};

        let position_struct = ScSpecEntry::UdtStructV0(ScSpecUdtStructV0 {
            doc: "".try_into().unwrap(),
            lib: "".try_into().unwrap(),
            name: "Position".try_into().unwrap(),
            fields: vec![ScSpecUdtStructFieldV0 {
                doc: "".try_into().unwrap(),
                name: "amount".try_into().unwrap(),
                type_: ScSpecTypeDef::I128,
            }]
            .try_into()
            .unwrap(),
        });

        let old = spec_of(&[
            func("withdraw", &[], None),
            event(
                "burn",
                &[(
                    "amount",
                    ScSpecTypeDef::I128,
                    ScSpecEventParamLocationV0::Data,
                )],
            ),
            position_struct.clone(),
        ]);

        let new = spec_of(&[
            func("pause", &[], None),
            event(
                "mint",
                &[(
                    "amount",
                    ScSpecTypeDef::I128,
                    ScSpecEventParamLocationV0::Data,
                )],
            ),
        ]);

        let d = SpecDiff::between(&old, &new);
        assert!(d.breaking);
        assert_eq!(d.max_severity, Severity::Breaking);
        assert_eq!(d.functions.removed.len(), 1);
        assert_eq!(d.functions.added.len(), 1);
        assert_eq!(d.events.removed.len(), 1);
        assert_eq!(d.events.added.len(), 1);
        assert_eq!(d.types.removed.len(), 1);
    }
}
