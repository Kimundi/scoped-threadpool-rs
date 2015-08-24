extern crate rustc_version;

fn main() {
    if rustc_version::version_matches(">= 1.4.0") {
        println!("cargo:rustc-cfg=compiler_has_scoped_bugfix");
    }
}
