#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Fuzz the strkey parser with arbitrary input
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = stellar_strkey::Strkey::from_string(s);
    }
});
