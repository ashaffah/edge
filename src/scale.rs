//! scale — diagnose scale serial output (GSC, Fujitsu, Mettler, etc.)
//!
//! Run as an edge-client subcommand:
//!   edge-client scale                           # pick a port interactively from the list
//!   edge-client scale --port COM3               # go straight to COM3 @ 9600 baud
//!   edge-client scale --port COM3 --baud 4800   # try another baud rate
//!   edge-client scale --port COM3 --scan        # try all common baud rates (3s each)
//!   edge-client scale --port COM3 --send "T\r\n"  # send tare before reading
//!   edge-client scale --port COM3 --duration 10   # stop after 10 seconds
//!   edge-client scale --list                    # show all COM ports then exit
//!
//! Each output line: `HEX  |  printable-ASCII`
//!   - Readable text          → baud rate correct
//!   - Random/garbled chars   → wrong baud rate
//!   - Non-ASCII bytes (dots)  → the scale uses a custom encoding (e.g. GSC 0xB0-0xB9)

use std::io::Write;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio_serial::SerialPortBuilderExt;

// Baud rates commonly used by industrial scales
const COMMON_BAUDS: &[u32] = &[9600, 4800, 2400, 19200, 1200, 38400, 57600, 115200];

// Scan each baud rate for 3 seconds
const SCAN_DURATION_SECS: f64 = 3.0;

#[derive(clap::Args)]
pub struct ScaleArgs {
    /// COM port to open.
    /// If omitted, show the port list and pick interactively.
    #[arg(long)]
    port: Option<String>,

    /// Baud rate
    #[arg(long, default_value_t = 9600)]
    baud: u32,

    /// Try all common baud rates automatically (3 seconds each)
    #[arg(long)]
    scan: bool,

    /// Stop after N seconds (default: keep running until Ctrl+C)
    #[arg(long)]
    duration: Option<f64>,

    /// Bytes to send to the scale before starting to read.
    /// Escape sequences: \\r \\n \\t \\xNN — example: "T\\r\\n" for tare
    #[arg(long, default_value = "")]
    send: String,

    /// Show all available COM ports then exit
    #[arg(long)]
    list: bool,
}

pub async fn run(args: ScaleArgs) {
    diagnose(args).await;

    // On Windows (double-click): keep the window from closing immediately
    #[cfg(windows)]
    {
        print!("\nPress Enter to close this window...");
        let _ = std::io::stdout().flush();
        let mut buf = String::new();
        let _ = std::io::stdin().read_line(&mut buf);
    }
}

async fn diagnose(args: ScaleArgs) {
    if args.list {
        list_ports();
        return;
    }

    // If --port is omitted, show the list and pick interactively
    let port = match args.port {
        Some(p) => p,
        None => match select_port() {
            Some(p) => p,
            None => return,
        },
    };

    let send_cmd: Option<Vec<u8>> = if args.send.is_empty() {
        None
    } else {
        Some(parse_escape(&args.send))
    };

    if args.scan {
        scan(&port, send_cmd.as_deref()).await;
        return;
    }

    listen(&port, args.baud, args.duration, send_cmd.as_deref()).await;
}

// ---------------------------------------------------------------------------
// List ports
// ---------------------------------------------------------------------------

fn format_port_type(pt: &tokio_serial::SerialPortType) -> String {
    use tokio_serial::SerialPortType::*;
    match pt {
        UsbPort(info) => {
            // Show manufacturer + product if present, fall back to VID:PID
            let parts: Vec<&str> = [info.manufacturer.as_deref(), info.product.as_deref()]
                .iter()
                .filter_map(|s| s.filter(|s| !s.is_empty()))
                .collect();

            if parts.is_empty() {
                format!("USB VID:{:04x} PID:{:04x}", info.vid, info.pid)
            } else {
                parts.join(" ")
            }
        }
        BluetoothPort => "Bluetooth".into(),
        PciPort => "PCI".into(),
        Unknown => "Unknown".into(),
    }
}

fn list_ports() {
    match tokio_serial::available_ports() {
        Ok(ports) if ports.is_empty() => println!("(no COM ports detected)\n"),
        Ok(ports) => {
            println!("Available COM ports:");
            for p in &ports {
                println!("  {:<10}  {}", p.port_name, format_port_type(&p.port_type));
            }
            println!();
        }
        Err(e) => eprintln!("(error listing ports: {e})\n"),
    }
}

// ---------------------------------------------------------------------------
// Interactive port picker — shown when --port is omitted
// ---------------------------------------------------------------------------

fn select_port() -> Option<String> {
    let ports = match tokio_serial::available_ports() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Error listing ports: {e}");
            return None;
        }
    };

    if ports.is_empty() {
        println!("(no COM ports detected — connect the scale then run again)");
        return None;
    }

    println!("Available ports:");
    for (i, p) in ports.iter().enumerate() {
        println!(
            "  [{}] {:<10}  {}",
            i + 1,
            p.port_name,
            format_port_type(&p.port_type)
        );
    }
    println!();

    loop {
        print!("Select port [1-{}]: ", ports.len());
        let _ = std::io::stdout().flush();

        let mut input = String::new();
        if std::io::stdin().read_line(&mut input).is_err() {
            return None;
        }

        let trimmed = input.trim();

        // Enter with no input → use the first port as the default
        if trimmed.is_empty() {
            let port = &ports[0];
            println!("→ {}", port.port_name);
            return Some(port.port_name.clone());
        }

        if let Ok(n) = trimmed.parse::<usize>()
            && n >= 1
            && n <= ports.len()
        {
            println!("→ {}", ports[n - 1].port_name);
            return Some(ports[n - 1].port_name.clone());
        }

        eprintln!("  Invalid input — enter a number from 1 to {}", ports.len());
    }
}

// ---------------------------------------------------------------------------
// Listen
// ---------------------------------------------------------------------------

async fn listen(port: &str, baud: u32, duration: Option<f64>, send_cmd: Option<&[u8]>) -> usize {
    let serial = match tokio_serial::new(port, baud)
        .timeout(Duration::from_millis(100))
        .open_native_async()
    {
        Ok(s) => s,
        Err(e) => {
            eprintln!("  [ERROR opening {port} @ {baud}: {e}]");
            return 0;
        }
    };

    match duration {
        None => println!("--- {port} @ {baud} baud, listening (Ctrl+C to stop) ---"),
        Some(d) => println!("--- {port} @ {baud} baud, listening {d:.0}s ---"),
    }

    let mut serial = serial;
    let mut total = 0usize;
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 256];

    // Send the command if there is one
    if let Some(cmd) = send_cmd {
        if let Err(e) = tokio::io::AsyncWriteExt::write_all(&mut serial, cmd).await {
            eprintln!("  (send error: {e})");
        } else {
            println!("  (sent: {:?})", String::from_utf8_lossy(cmd));
        }
    }

    let deadline = duration.map(|d| Instant::now() + Duration::from_secs_f64(d));

    loop {
        // Check the deadline
        if let Some(dl) = deadline
            && Instant::now() >= dl
        {
            break;
        }

        // Read with a 100ms timeout — so we can check the deadline
        let read_result =
            tokio::time::timeout(Duration::from_millis(100), serial.read(&mut chunk)).await;

        let n = match read_result {
            Ok(Ok(0)) => break, // EOF
            Ok(Ok(n)) => n,
            Ok(Err(_)) | Err(_) => continue, // timeout or read error → try again
        };

        total += n;
        buf.extend_from_slice(&chunk[..n]);

        // Flush at line boundaries — check \r\n first, then \n, then \r
        // (order matters: \r\n must be treated as ONE terminator)
        for term in [b"\r\n".as_slice(), b"\n", b"\r"] {
            while let Some(pos) = buf.windows(term.len()).position(|w| w == term) {
                let line = buf[..pos].to_vec();
                println!("{}", format_chunk(&line));
                buf.drain(..pos + term.len());
            }
        }

        // Flush every 64 bytes for binary streams (no line terminator)
        if buf.len() >= 64 {
            println!("{}", format_chunk(&buf));
            buf.clear();
        }
    }

    // Flush the remaining buffer
    if !buf.is_empty() {
        println!("{}", format_chunk(&buf));
    }

    println!("--- {total} bytes total ---\n");
    total
}

// ---------------------------------------------------------------------------
// Scan
// ---------------------------------------------------------------------------

async fn scan(port: &str, send_cmd: Option<&[u8]>) {
    println!("Scanning common baud rates on {port} ({SCAN_DURATION_SECS:.0}s each)...\n");

    let mut results: Vec<(u32, usize)> = Vec::new();

    for &baud in COMMON_BAUDS {
        let n = listen(port, baud, Some(SCAN_DURATION_SECS), send_cmd).await;
        results.push((baud, n));
    }

    // Sort: the baud with the most bytes first
    results.sort_by_key(|b| std::cmp::Reverse(b.1));

    println!("=== Scan summary (sorted by bytes received) ===");
    for (baud, n) in &results {
        let marker = if *n > 0 { "  <-- candidate" } else { "" };
        println!("  {baud:>6} baud  =>  {n:>5} bytes{marker}");
    }
    println!();
    println!("Look at the per-baud output above — the correct baud is the one whose ASCII");
    println!("column shows readable text like '+0013.81 kg' or 'WT:  23.50 kg', not random chars.");
    println!();
    println!("GSC tip: if you see dots (.....) even though the baud is correct,");
    println!("the scale uses a non-ASCII encoding (0xB0-0xB9 = digits 0-9).");
    println!("Add a byte_map in mapping.json — no need to change the baud.");
}

// ---------------------------------------------------------------------------
// Format output: hex | ASCII (printable)
// ---------------------------------------------------------------------------

fn format_chunk(data: &[u8]) -> String {
    let hex: String = data
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ");
    let ascii: String = data
        .iter()
        .map(|&b| {
            if (32..127).contains(&b) {
                b as char
            } else {
                '.'
            }
        })
        .collect();
    format!("{hex:<56}  |  {ascii}")
}

// ---------------------------------------------------------------------------
// Parse escape sequences: \r \n \t \\ \xNN
// ---------------------------------------------------------------------------

fn parse_escape(s: &str) -> Vec<u8> {
    let mut out = Vec::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.extend(c.to_string().as_bytes());
            continue;
        }
        match chars.next() {
            Some('r') => out.push(b'\r'),
            Some('n') => out.push(b'\n'),
            Some('t') => out.push(b'\t'),
            Some('\\') => out.push(b'\\'),
            Some('x') => {
                let h: String = chars.by_ref().take(2).collect();
                if let Ok(b) = u8::from_str_radix(&h, 16) {
                    out.push(b);
                }
            }
            Some(other) => {
                out.push(b'\\');
                out.extend(other.to_string().as_bytes());
            }
            None => out.push(b'\\'),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_chunk_mixed() {
        let data = b"GS12.50kg";
        let out = format_chunk(data);
        assert!(out.contains("47 53 31 32 2e 35 30 6b 67"));
        assert!(out.contains("GS12.50kg"));
    }

    #[test]
    fn format_chunk_non_ascii() {
        // 0xB0 is not printable → shown as a dot
        let data = &[b'G', b'S', 0xB0, 0xB1, b'.', b'5', b'0'];
        let out = format_chunk(data);
        assert!(out.contains("..")); // 0xB0 and 0xB1 become dots
        assert!(out.contains("GS"));
    }

    #[test]
    fn parse_escape_crlf() {
        assert_eq!(parse_escape("T\\r\\n"), b"T\r\n");
        assert_eq!(parse_escape("Z\\r\\n"), b"Z\r\n");
    }

    #[test]
    fn parse_escape_hex() {
        assert_eq!(parse_escape("\\x0d\\x0a"), b"\r\n");
    }

    #[test]
    fn parse_escape_plain() {
        assert_eq!(parse_escape("TARE"), b"TARE");
    }
}
