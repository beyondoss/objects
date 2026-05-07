fn main() {
    let cmd = std::env::args().nth(1);
    match cmd.as_deref() {
        Some("generate-openapi") => {
            eprintln!("generate-openapi: not yet implemented");
            std::process::exit(1);
        }
        _ => {
            eprintln!("usage: xtask <generate-openapi>");
            std::process::exit(1);
        }
    }
}
