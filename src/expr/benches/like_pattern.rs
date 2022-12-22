// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/cli/gen-lints.py first.
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::needless_collect)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![deny(warnings)]
#![deny(clippy::bool_comparison)]
#![deny(clippy::clone_on_ref_ptr)]
#![deny(clippy::no_effect)]
#![deny(clippy::unnecessary_unwrap)]
#![deny(clippy::dbg_macro)]
#![deny(clippy::todo)]
#![deny(clippy::wildcard_dependencies)]
#![deny(clippy::zero_prefixed_literal)]
#![deny(clippy::borrowed_box)]
#![deny(clippy::deref_addrof)]
#![deny(clippy::double_must_use)]
#![deny(clippy::double_parens)]
#![deny(clippy::extra_unused_lifetimes)]
#![deny(clippy::needless_borrow)]
#![deny(clippy::needless_question_mark)]
#![deny(clippy::needless_return)]
#![deny(clippy::redundant_pattern)]
#![deny(clippy::redundant_slicing)]
#![deny(clippy::redundant_static_lifetimes)]
#![deny(clippy::single_component_path_imports)]
#![deny(clippy::unnecessary_cast)]
#![deny(clippy::useless_asref)]
#![deny(clippy::useless_conversion)]
#![deny(clippy::builtin_type_shadow)]
#![deny(clippy::duplicate_underscore_argument)]
#![deny(clippy::double_neg)]
#![deny(clippy::unnecessary_mut_passed)]
#![deny(clippy::wildcard_in_or_patterns)]
#![deny(clippy::collapsible_if)]
#![deny(clippy::collapsible_else_if)]
#![deny(clippy::crosspointer_transmute)]
#![deny(clippy::excessive_precision)]
#![deny(clippy::overflow_check_conditional)]
#![deny(clippy::as_conversions)]
#![deny(clippy::match_overlapping_arm)]
#![deny(clippy::zero_divided_by_zero)]
#![deny(clippy::must_use_unit)]
#![deny(clippy::suspicious_assignment_formatting)]
#![deny(clippy::suspicious_else_formatting)]
#![deny(clippy::suspicious_unary_op_formatting)]
#![deny(clippy::mut_mutex_lock)]
#![deny(clippy::print_literal)]
#![deny(clippy::same_item_push)]
#![deny(clippy::useless_format)]
#![deny(clippy::write_literal)]
#![deny(clippy::redundant_closure)]
#![deny(clippy::redundant_closure_call)]
#![deny(clippy::unnecessary_lazy_evaluations)]
#![deny(clippy::partialeq_ne_impl)]
#![deny(clippy::redundant_field_names)]
#![deny(clippy::transmutes_expressible_as_ptr_casts)]
#![deny(clippy::unused_async)]
#![deny(clippy::disallowed_methods)]
#![deny(clippy::disallowed_macros)]
#![deny(clippy::from_over_into)]
// END LINT CONFIG

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mz_expr::like_pattern;

// Ozymandias, by Percy Bysshe Shelley
// written in 1818, in the public domain.
const POEM: &[&str] = &[
    "I met a traveller from an antique land,",
    "Who said -- Two vast and trunkless legs of stone",
    "Stand in the desert... Near them, on the sand,",
    "Half sunk a shattered visage lies, whose frown,",
    "And wrinkled lip, and sneer of cold command,",
    "Tell that its sculptor well those passions read",
    "Which yet survive, stamped on these lifeless things,",
    "The hand that mocked them, and the heart that fed;",
    "And on the pedestal, these words appear:",
    "My name is Ozymandias, King of Kings;",
    "Look on my Works, ye Mighty, and despair!",
    "Nothing beside remains. Round the decay",
    "Of that colossal Wreck, boundless and bare",
    "The lone and level sands stretch far away.",
];

fn search_poem(needle: &like_pattern::Matcher) {
    for i in 0..POEM.len() {
        needle.is_match(black_box(POEM[i]));
    }
}

fn bench_op<F>(c: &mut Criterion, name: &str, mut compile_fn: F)
where
    F: FnMut(&str) -> like_pattern::Matcher,
{
    let mut group = c.benchmark_group("like_pattern");

    // Test how long it takes to compile a pattern.
    group.bench_function(format!("{}_compile", name), |b| {
        b.iter(|| compile_fn(black_box("a%b_c%d_e")))
    });

    // Test some search scenarios:
    let mut matcher = compile_fn("And");
    group.bench_function(format!("{}_search_literal", name), |b| {
        b.iter(|| search_poem(&matcher))
    });
    matcher = compile_fn("And%");
    group.bench_function(format!("{}_search_starts_with", name), |b| {
        b.iter(|| search_poem(&matcher))
    });
    matcher = compile_fn("%and%");
    group.bench_function(format!("{}_search_contains", name), |b| {
        b.iter(|| search_poem(&matcher))
    });
    matcher = compile_fn("%and%the%");
    group.bench_function(format!("{}_search_contains2", name), |b| {
        b.iter(|| search_poem(&matcher))
    });
    matcher = compile_fn("%and%the%th%");
    group.bench_function(format!("{}_search_contains3", name), |b| {
        b.iter(|| search_poem(&matcher))
    });
    matcher = compile_fn("%!");
    group.bench_function(format!("{}_search_ends_with", name), |b| {
        b.iter(|| search_poem(&matcher))
    });
    matcher = compile_fn("%e%e%e%e%e?");
    group.bench_function(format!("{}_search_adversarial", name), |b| {
        b.iter(|| matcher.is_match(black_box("wheeeeeeeeeeeeeeeeeeeeee!")))
    });
}

pub fn bench_ilike(c: &mut Criterion) {
    bench_op(c, "ilike", |pattern| {
        like_pattern::compile(pattern, true).unwrap()
    });
}

pub fn bench_like(c: &mut Criterion) {
    bench_op(c, "like", |pattern| {
        like_pattern::compile(pattern, false).unwrap()
    });
}

criterion_group!(benches, bench_like, bench_ilike);
criterion_main!(benches);
