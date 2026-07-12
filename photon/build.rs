//! Build script for the photon facade (stub codegen placeholder).

fn main() {
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    std::fs::write(
        out_dir.join("generated_models.rs"),
        "// Photon facade — ops metadata codegen lives in integration hosts\n",
    )
    .expect("write stub generated_models.rs");
}
