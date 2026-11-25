#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Fuzz the contract spec parser with arbitrary XDR bytes
    let _ = lumenqraph_core::ContractSpec::from_spec_xdr(data);
});
