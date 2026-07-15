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
//! A change is [`breaking`](SpecDiff::breaking) if it can invalidate an existing
//! integration: anything removed or changed. Purely additive upgrades are not.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use serde_json::{json, Value};

use crate::spec::ContractSpec;

/// The difference between two parsed interfaces, oldest to newest.
#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct SpecDiff {
    /// True if anything was removed or changed — i.e. an integration built
    /// against the old interface may no longer work.
    pub breaking: bool,
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
    /// Signatures present only in the new interface.
    pub added: Vec<String>,
    /// Signatures present only in the old interface.
    pub removed: Vec<String>,
    /// Items whose name persisted but whose signature moved.
    pub changed: Vec<ChangedItem>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ChangedItem {
    pub name: String,
    pub from: String,
    pub to: String,
}

impl SectionDiff {
    fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.changed.is_empty()
    }

    /// Additions can't break an existing caller; removals and changes can.
    fn has_breaking(&self) -> bool {
        !self.removed.is_empty() || !self.changed.is_empty()
    }
}

impl SpecDiff {
    /// Diff `old` against `new`. The result reads in the direction of the
    /// upgrade: `added` means "new interface has it, old one didn't".
    pub fn between(old: &ContractSpec, new: &ContractSpec) -> Self {
        let functions = diff_section(&function_sigs(old), &function_sigs(new));
        let events = diff_section(&event_sigs(old), &event_sigs(new));
        let types = diff_section(&type_sigs(old), &type_sigs(new));

        let mut diff = SpecDiff {
            breaking: functions.has_breaking() || events.has_breaking() || types.has_breaking(),
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
fn diff_section(old: &BTreeMap<String, String>, new: &BTreeMap<String, String>) -> SectionDiff {
    let names: BTreeSet<&String> = old.keys().chain(new.keys()).collect();
    let mut diff = SectionDiff::default();

    for name in names {
        match (old.get(name), new.get(name)) {
            (Some(before), Some(after)) if before != after => diff.changed.push(ChangedItem {
                name: name.clone(),
                from: before.clone(),
                to: after.clone(),
            }),
            (Some(_), Some(_)) => {}
            (Some(before), None) => diff.removed.push(before.clone()),
            (None, Some(after)) => diff.added.push(after.clone()),
            (None, None) => unreachable!("name came from one of the two maps"),
        }
    }
    diff
}

fn function_sigs(spec: &ContractSpec) -> BTreeMap<String, String> {
    spec.functions
        .iter()
        .map(|f| {
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
            (
                f.name.clone(),
                format!("{}({}) -> {}", f.name, inputs.join(", "), output),
            )
        })
        .collect()
}

/// Event signatures carry each param's location and the body's data format:
/// moving a param from topic to data, or switching the body layout, silently
/// breaks every consumer decoding that event, so both belong in the identity.
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
        assert_eq!(d.functions.removed, vec!["withdraw(amount: i128) -> void"]);
    }

    #[test]
    fn a_retyped_parameter_is_a_change_not_an_add_and_remove() {
        let old = spec_of(&[func("mint", &[("amount", ScSpecTypeDef::I128)], None)]);
        let new = spec_of(&[func("mint", &[("amount", ScSpecTypeDef::U128)], None)]);
        let d = SpecDiff::between(&old, &new);
        assert!(d.breaking);
        assert!(d.functions.added.is_empty());
        assert!(d.functions.removed.is_empty());
        assert_eq!(
            d.functions.changed,
            vec![ChangedItem {
                name: "mint".into(),
                from: "mint(amount: i128) -> void".into(),
                to: "mint(amount: u128) -> void".into(),
            }]
        );
    }

    #[test]
    fn a_changed_return_type_is_breaking() {
        let old = spec_of(&[func("decimals", &[], Some(ScSpecTypeDef::U32))]);
        let new = spec_of(&[func("decimals", &[], Some(ScSpecTypeDef::U64))]);
        let d = SpecDiff::between(&old, &new);
        assert!(d.breaking);
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
}
