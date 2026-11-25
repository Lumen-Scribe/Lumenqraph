#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Fuzz the XDR decoder with arbitrary bytes
    let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, data);
    let _ = lumenqraph_core::xdr::decode_scval_base64(&b64);
});
