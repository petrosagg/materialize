// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

#![no_main]

#[macro_use]
extern crate libfuzzer_sys;

// We're just looking for crashes here, not parse failures
fuzz_target!(|data: &[u8]| {
    if let Ok(string) = std::str::from_utf8(data) {
        let mut state = sqllogictest::State::new();
        for record in sqllogictest::parse_records(&string) {
            match record {
                Ok(record) => sqllogictest::run_record(&mut state, &record),
                _ => (),
            }
        }
    };
});
