use std::error::Error;
use std::process::Command;

/// This binary executes all the nodes in the project in order.
pub fn main() -> Result<(), Box<dyn Error>> {
    let nodes = ["127.0.0.1:808", "127.0.0.1:809"];

    for i in 0..8 {
        Command::new("cmd")
            .arg("/C")
            .arg("start") // Abre una nueva ventana
            .arg("cmd") // Abre una nueva instancia de `cmd.exe`
            .arg("/K") // Mantiene la ventana abierta después de ejecutar el comando
            .arg(format!(
                "cargo run --bin node -- {}{} {}{}",
                nodes[0], i, nodes[1], i
            ))
            .spawn()
            .expect("Failed to spawn terminal");
    }

    Ok(())
}
