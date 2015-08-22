use std::process::Command;

fn main() {
    let out = Command::new("rustc")
        .arg("--version")
        .output()
        .unwrap_or_else(|e| { panic!("failed to execute process: {}", e) });

    let out = String::from_utf8(out.stdout).unwrap();

    let chars = &['.', '-', ' '][..];
    let mut split = out[6..].split(chars);
    let _major = split.next().unwrap().parse::<u32>().unwrap();
    let minor = split.next().unwrap().parse::<u32>().unwrap();
    let _patch = split.next().unwrap().parse::<u32>().unwrap();

    if minor >= 4 {
        println!("cargo:rustc-cfg=compiler_has_scoped_bugfix");
    }
}
