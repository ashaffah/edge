# edge

Edge agent Rust untuk gateway IoT industri. Agent ini melakukan polling **Modbus**
(TCP / RTU) dari PLC dan membaca **timbangan serial-ASCII**, lalu mem-publish tiap
parameter ke **MQTT** — satu topic per nilai. Semua konfigurasi mesin (ID, koneksi,
register, interval polling) berada di satu file JSON mapping; tidak ada kompilasi
ulang per mesin.

Dirancang untuk berjalan di board Linux ARM/x86 kecil (Raspberry Pi dan sejenisnya)
sebagai service systemd yang hidup terus-menerus.

> 🇬🇧 English: [README.md](README.md)

## Daftar Isi

- [Struktur](#struktur)
- [Mulai cepat](#mulai-cepat)
- [Konfigurasi (`.env`)](#konfigurasi-env)
- [File mapping](#file-mapping)
  - [Jenis device & koneksi](#jenis-device--koneksi)
  - [Parameter & kategori](#parameter--kategori)
  - [Binding Modbus](#binding-modbus)
  - [Byte order](#byte-order)
  - [Parser weigher & command](#parser-weigher--command)
  - [Catatan timbangan](#catatan-timbangan)
  - [Aturan bridge](#aturan-bridge)
- [Tata letak topic MQTT](#tata-letak-topic-mqtt)
- [Control gate](#control-gate)
- [Diagnosa timbangan (`scale`)](#diagnosa-timbangan-scale)
- [Cross-compilation](#cross-compilation)
- [Lisensi](#lisensi)

## Struktur

Satu package `edge-client` (source di [`src/`](src/)):

| Path                           | Isinya                                                                        |
| ------------------------------ | ----------------------------------------------------------------------------- |
| [`src/`](src/)                 | Agent-nya: polling Modbus + baca weigher → publish MQTT, control gate, health |
| [`src/shared/`](src/shared/)   | Domain type mapping/Modbus (skema JSON mapping + semantik register)           |
| [`src/scale.rs`](src/scale.rs) | Subcommand `edge-client scale` — diagnosa output serial timbangan mentah      |

Satu instance `edge-client` menjalankan **satu mesin** (meski mesin itu bisa
mencakup PLC utama plus beberapa slave Modbus dan weigher di board yang sama).
Agent tidak melakukan persistence — semuanya di-publish ke MQTT dan dikonsumsi di
tempat lain.

Secara internal, agent men-spawn task konkuren via `tokio::select!`: driver event
loop MQTT, publisher heartbeat/resource/telemetry/PLC-status/weigher, dan control
subscriber. Akses Modbus RTU diserialisasi lewat satu task actor per-port sehingga
satu port serial fisik dibuka tepat sekali.

## Mulai cepat

```bash
# 1. Build
cargo build --release

# 2. Konfigurasi
cp deploy/edge-client/.env.example-edge-client .env       # kredensial broker
cp example-mapping.json ./mapping.json                     # device & register
# edit .env (host/user/pass MQTT) dan mapping.json (IP PLC, path serial, register)

# 3. Jalankan
MQTT_MAPPING_PATH=./mapping.json ./target/release/edge-client
```

Contoh siap pakai ada di root repo: `example-mapping.json`,
`example-mapping-plc-only.json`, `example-mapping-weigher-only.json`,
`example-mapping-plc-slave-weigher.json`.

## Konfigurasi (`.env`)

Konfigurasi mesin ada di JSON mapping. File `.env` hanya berisi kredensial koneksi
eksternal dan beberapa pengaturan runtime.

| Variable              | Default          | Deskripsi                                                            |
| --------------------- | ---------------- | -------------------------------------------------------------------- |
| `MQTT_HOST`           | _(wajib)_        | Host broker (atau full host untuk ws/wss)                            |
| `MQTT_PORT`           | _(wajib)_        | Port broker (mis. 1883, 8883, 443)                                   |
| `MQTT_USERNAME`       | _(wajib)_        | Username broker                                                      |
| `MQTT_PASSWORD`       | _(wajib)_        | Password broker                                                      |
| `MQTT_CLIENT_ID`      | _(wajib)_        | Client id unik                                                       |
| `MQTT_PROTOCOL`       | `mqtt`           | `mqtt` (TCP), `mqtts` (TLS), `ws`, `wss`                             |
| `MQTT_WEBSOCKET_PATH` | `/mqtt`          | Path WebSocket (khusus ws/wss)                                       |
| `MQTT_KEEPALIVE`      | `60`             | Keepalive detik; LWT fire setelah timeout ini saat berhenti mendadak |
| `MQTT_PUBLISH_QOS`    | `1`              | QoS publish: `0`, `1`, atau `2`                                      |
| `MQTT_MAPPING_PATH`   | `./mapping.json` | Path ke JSON mapping                                                 |
| `CACHE_URL`           | _(kosong)_       | URL Valkey/Redis untuk control gate; kosong = semua control di-deny  |
| `CONTROL_GATE_KEY`    | `control`        | Hash key Valkey untuk control gate                                   |
| `RUST_LOG`            | `info`           | Filter tracing (mis. `info`, `edge_client=debug`)                    |

## File mapping

Satu dokumen JSON mendeskripsikan setiap device di mesin. Agent menurunkan tiap
topic MQTT sebagai `{base_topic}/{location}/{name}/{topic}`.

```jsonc
{
  "base_topic": "acme/site",
  "poll_interval_ms": 1000,
  "devices": [
    /* ... */
  ],
  "bridge": [
    /* ... */
  ], // opsional
}
```

Machine id adalah `{location}/{name}` dari device `master` pertama. Kalau tidak
ada, dipakai `slave` pertama, lalu `weigher` pertama.

### Jenis device & koneksi

Tiap entry di `devices[]` punya `device_type`, `device_id` yang stabil (dipakai
untuk routing dan referensi bridge), `location`, `name`, dan `connection`.

| `device_type` | Peran                                                               |
| ------------- | ------------------------------------------------------------------- |
| `master`      | PLC utama — menentukan machine id dan transport Modbus utama        |
| `slave`       | Device Modbus tambahan (chiller RS-485, panel sensor, PLC kedua, …) |
| `weigher`     | Timbangan serial-ASCII (tanpa Modbus)                               |

Jenis koneksi (`connection.type`):

```jsonc
// Modbus TCP
{ "type": "tcp", "host": "192.168.10.10", "port": 502, "unit_id": 1 }

// Modbus RTU lewat port serial (USB-RS485, dll.)
{ "type": "rtu_serial", "unit_id": 8, "path": "/dev/ttyUSB0",
  "baud": 19200, "parity": "none", "stop_bits": 1, "data_bits": 8 }

// Serial ASCII (timbangan weigher)
{ "type": "serial_ascii", "path": "COM9",
  "baud": 9600, "parity": "none", "stop_bits": 1, "data_bits": 7 }
```

Beberapa slave di bus RS-485 yang sama (`path` sama) berbagi satu task actor dan
dibedakan lewat `unit_id`, jadi port hanya dibuka sekali. Koneksi `tcp` memakai
Modbus TCP, jadi converter Ethernet ke RS-485 harus berjalan sebagai gateway
Modbus TCP. Raw RTU lewat TCP (mode transparent converter) sudah ada sebagai
transport di kode, tapi belum bisa dipilih dari mapping.

### Parameter & kategori

Tiap device mendeklarasikan parameter di tiga array:

- **`monitoring[]`** — dibaca dan di-publish sebagai telemetry.
- **`set[]`** — setpoint writable (float/integer), ditulis saat publish MQTT. **Tanpa** gate.
- **`control[]`** — on/off aktuator (register atau coil), ditulis saat publish MQTT. **Dilindungi gate.**

Satu parameter berbentuk:

```jsonc
{
  "key": "temp_tank", // key unik (juga nama capture group regex weigher)
  "label": "Tank Temperature", // label manusiawi
  "topic": "temp/tank", // suffix topic
  "type": "float", // float | integer | boolean | string
  "unit": "C", // opsional
  "modbus": {
    /* binding — lihat bawah; kosongkan untuk param weigher */
  },
}
```

Parameter tanpa binding `modbus` tidak di-poll lewat Modbus (dipakai untuk nilai
capture-group weigher).

### Binding Modbus

`modbus.kind` memetakan langsung ke Modbus function code — tanpa inferensi:

| `kind`                     | FC   | Kegunaan                                              |
| -------------------------- | ---- | ----------------------------------------------------- |
| `read_coils`               | FC1  | Bulk read bit boolean dari address 0 (`monitoring`)   |
| `read_discrete_inputs`     | FC2  | Read single-bit terarah (`monitoring`)                |
| `read_holding_registers`   | FC3  | Bulk read float/integer dari address 0 (`monitoring`) |
| `read_input_registers`     | FC4  | Read terarah di address sparse (`monitoring`)         |
| `write_single_coil`        | FC5  | Tulis 1 bit boolean (`control`)                       |
| `write_single_register`    | FC6  | Tulis 1 register dengan nilai on/off (`control`)      |
| `write_multiple_registers` | FC16 | Tulis 2 register (setpoint float32/int32) (`set`)     |

Contoh:

```jsonc
// FC3 monitoring float, byte order big/little
{ "kind": "read_holding_registers", "address": 10, "byte_order": "big_little" }

// FC4 dengan multiplier scale (mis. raw 250 → 25.0)
{ "kind": "read_input_registers", "address": 1000, "scale": 0.1 }

// FC5 control coil
{ "kind": "write_single_coil", "address": 4 }

// FC6 control register: nilai 1 saat true, 3 saat false
{ "kind": "write_single_register", "address": 6, "on_value": 1, "off_value": 3 }

// FC16 setpoint (2 register)
{ "kind": "write_multiple_registers", "address": 26, "byte_order": "big_big" }
```

- `read_holding_registers` dan `read_input_registers` men-decode nilai 32-bit dari
  2 register. Dengan `scale`, `read_input_registers` malah membaca 1 register lalu
  mengalikan.
- `off_value` default `0` kalau tidak diisi.

### Byte order

Nilai 32-bit menempati dua register 16-bit; vendor berbeda dalam urutan packing:

| `byte_order`    | Byte | Word | Vendor umum                         |
| --------------- | ---- | ---- | ----------------------------------- |
| `big_big`       | BE   | BE   | "network order" — Schneider, ABB    |
| `little_big`    | LE   | BE   | Siemens S7                          |
| `big_little`    | BE   | LE   | Schneider Quantum, sebagian Modicon |
| `little_little` | LE   | LE   | sebagian Mitsubishi / generik       |

Default `big_big` kalau tidak diisi. Kalau nilai ter-decode jadi sampah, coba
tukar urutan byte dan/atau word.

### Parser weigher & command

Device `weigher` membaca baris ASCII dari serial dan mem-parse-nya dengan regex
memakai **named capture group**; tiap nama group harus cocok dengan `key`
`monitoring`.

```jsonc
"parser": {
  "regex": "[A-Za-z-]*(?P<weight>[-+]?[0-9]+\\.[0-9]+)(?P<unit>[A-Za-z]*)",
  "byte_map": [ { "from": 176, "to": 48 } /* 0xB0→'0' … 0xB9→'9' */ ],
  "raw_topic": "raw",          // opsional: publish juga semua group sebagai 1 objek JSON
  "stable": { "group": "st", "equals": "ST" }, // opsional: publish hanya pembacaan stabil
  "read_timeout_ms": 5000       // opsional: reopen port jika sunyi selama ini
}
```

- `byte_map` memetakan ulang byte mentah sebelum decode ASCII — untuk brand yang
  meng-encode digit sebagai byte non-standard (mis. GSC memakai `0xB0`–`0xB9`
  untuk `'0'`–`'9'`). Kosongkan untuk brand ASCII standar (mis. Fujitsu).
- `raw_topic`, jika diisi, mem-publish `{"weight": 12.50, "unit": "kg"}` ke
  `{base}/{location}/{name}/{raw_topic}` di samping topic per-key.
- `stable` menggerbangi publish berdasarkan flag stabil/motion timbangan: tambah
  capture group untuk token status (mis. `(?P<st>ST|US)`) lalu set `group`/`equals`
  — hanya baris yang token-nya sama dengan `equals` yang di-publish, sehingga
  pembacaan saat bergerak dibuang. Hilangkan untuk mem-publish setiap baris.
- `read_timeout_ms` adalah watchdog idle: jika port tetap terbuka tapi tidak
  mengirim apa pun selama ini, koneksi di-reopen (menangkap port yang terbuka
  tapi sunyi). Hilangkan untuk timbangan poll/on-demand yang idle antar
  pembacaan; set beberapa kali interval output untuk timbangan kontinu.

`commands[]` opsional memungkinkan operator mengendalikan timbangan lewat serial
dari MQTT. Publish ke `{base}/{location}/{name}/cmd/{key}` menulis byte
`serial_cmd` command tersebut ke port (escape: `\r \n \t \\ \xNN`):

```jsonc
"commands": [
  { "key": "tare",  "serial_cmd": "T\\r\\n" },
  { "key": "zero",  "serial_cmd": "Z\\r\\n" },
  { "key": "print", "serial_cmd": "P\\r\\n" }
]
```

### Catatan timbangan

Pengaturan untuk timbangan di ruang timbang. Semuanya device `weigher` dengan
koneksi `serial_ascii`, tanpa Modbus. Blok `connection` berbeda per brand, jadi
jangan disalin ke timbangan lain tanpa dicek dulu.

#### GSC

- Serial: 9600 baud, 7 data bits, no parity, 1 stop bit.
- Digit dikirim sebagai byte `0xB0` sampai `0xB9`, bukan ASCII `'0'` sampai
  `'9'`, jadi `byte_map` memetakan byte 176 sampai 185 ke 48 sampai 57.
- Regex punya group `st` opsional untuk token status `ST`/`US`. Nilainya ikut
  ter-publish di JSON `raw`, tapi tidak ada aturan `stable`, jadi baris yang
  masih bergerak tetap di-publish.
- Command diakhiri CR LF: `T` untuk tare, `Z` untuk zero, `P` untuk print.

```jsonc
"connection": { "type": "serial_ascii", "path": "COM3",
  "baud": 9600, "parity": "none", "stop_bits": 1, "data_bits": 7 },
"parser": {
  "regex": "(?:(?P<st>ST|US)\\D*?)?(?P<weight>[-+]?[0-9]+\\.[0-9]+)(?P<unit>[A-Za-z]*)",
  "byte_map": [
    { "from": 176, "to": 48 }, { "from": 177, "to": 49 },
    { "from": 178, "to": 50 }, { "from": 179, "to": 51 },
    { "from": 180, "to": 52 }, { "from": 181, "to": 53 },
    { "from": 182, "to": 54 }, { "from": 183, "to": 55 },
    { "from": 184, "to": 56 }, { "from": 185, "to": 57 }
  ],
  "raw_topic": "raw"
},
"commands": [
  { "key": "tare",  "serial_cmd": "T\\r\\n" },
  { "key": "zero",  "serial_cmd": "Z\\r\\n" },
  { "key": "print", "serial_cmd": "P\\r\\n" }
]
```

#### Fujitsu (timbangan analitik)

- Serial: 9600 baud, 8 data bits, no parity, 1 stop bit.
- Output ASCII standar, jadi `byte_map` dikosongkan.
- Tiap pembacaan berupa record multi-baris, satu field per baris: `TTYPE`, `ID`,
  `DATE`, `TIME`, `TEMP`, `BAT`, `MODE`, `REF`, `STATUS`, `STEP`, `TARE`,
  `ZERO`, `WT`. Baris berat berbentuk `WT:0.00g`, atau `WT:-0.12g` setelah tare.
  Satuannya gram.
- Parser bekerja per baris, jadi regex di-anchor ke `WT:`. Tanpa itu, baris
  seperti `TEMP:25.9C`, `BAT:7.1V` atau `REF:2000.00g` ikut ter-capture sebagai
  berat. `[-+]?` wajib ada, kalau tidak pembacaan negatif tidak match dan hilang
  tanpa peringatan.
- Aturan `stable` tidak bisa dipakai: `STATUS:STEADY` ada di baris yang berbeda
  dari `WT:`, jadi group status tidak pernah match di baris berat dan semua baris
  akan ditolak. Semua pembacaan di-publish.
- Command diakhiri CR LF: `T` untuk tare, `Z` untuk zero, `P` untuk print.

```jsonc
"connection": { "type": "serial_ascii", "path": "/dev/ttyUSB0",
  "baud": 9600, "parity": "none", "stop_bits": 1, "data_bits": 8 },
"parser": {
  "regex": "WT:\\s*(?P<weight>[-+]?[0-9.]+)\\s*(?P<unit>\\D+)",
  "byte_map": [],
  "raw_topic": "raw"
},
"commands": [
  { "key": "tare",  "serial_cmd": "T\\r\\n" },
  { "key": "zero",  "serial_cmd": "Z\\r\\n" },
  { "key": "print", "serial_cmd": "P\\r\\n" }
]
```

#### Radwag WLC/C2 (60 kg)

- Serial: 9600 baud, 8 data bits, no parity, 1 stop bit (default RS232 Radwag).
- Output ASCII standar, jadi `byte_map` dikosongkan.
- Regex sudah dicek langsung ke timbangan. Regex tidak di-anchor dan mengambil
  angka pertama (bulat atau desimal) yang diikuti satuan di posisi mana pun pada
  baris.
- Tidak ada aturan `stable`, jadi baris yang masih bergerak (ditandai `?`, `^`
  atau `v` oleh timbangan) ikut di-publish bersama baris yang stabil.
- Command diakhiri CR LF: `T` untuk tare, `Z` untuk zero, dan `print` mengirim
  `S` (command Radwag "send stable result").

```jsonc
"connection": { "type": "serial_ascii", "path": "COM4",
  "baud": 9600, "parity": "none", "stop_bits": 1, "data_bits": 8 },
"parser": {
  "regex": "(?P<weight>[-+]?\\d+(?:\\.\\d+)?)\\s*(?P<unit>[A-Za-z]+)",
  "byte_map": [],
  "raw_topic": "raw"
},
"commands": [
  { "key": "tare",  "serial_cmd": "T\\r\\n" },
  { "key": "zero",  "serial_cmd": "Z\\r\\n" },
  { "key": "print", "serial_cmd": "S\\r\\n" }
]
```

#### Gewinn XK3118T1

- Dibaca dari port DB9 RS232 indikator, yang terpisah dari port loadcell.
- Serial: 2400 baud, 8 data bits, no parity, 1 stop bit. Baud rate dikonfirmasi
  dengan Web Serial test tool.
- Indikator berjalan di mode komunikasi `Co 3`: stream kontinu berisi berat net
  dan satuan, sekitar 7.8 frame per detik, resolusi 0.02 kg, tiap frame diakhiri
  CR LF. Frame berbentuk `=0000.14(kg)`, nilai negatif seperti `=-000.06(kg)`.
  Regex melewati `(` di antara angka dan satuan.
- Frame `Co 3` tidak membawa flag stabil, jadi aturan `stable` tidak bisa
  dipakai. Deteksi stabil (window plus toleransi) belum diimplementasi.
- Command: manual XK3118T1 menyebut tare sebagai `STX E ETX` dan zero sebagai
  `STX D ETX`, untuk mode `Co 5` (command-response). Saat dites di `Co 3`,
  indikator tidak membalas dan stream tidak berubah. Mode `Co 5` belum dites.
- Manual tidak punya command print lewat serial. Print terikat ke tombol fisik
  akumulasi dan hanya keluar di mode `Co 6` saat tombol TOTAL ditekan. Command
  `print` dibiarkan dengan `serial_cmd` kosong sampai dikonfirmasi di device.

```jsonc
"connection": { "type": "serial_ascii", "path": "COM4",
  "baud": 2400, "parity": "none", "stop_bits": 1, "data_bits": 8 },
"parser": {
  "regex": "(?P<weight>[-+]?\\d+\\.\\d+)\\D*?(?P<unit>[A-Za-z]+)",
  "byte_map": [],
  "raw_topic": "raw"
},
"commands": [
  { "key": "tare",  "serial_cmd": "\\x02E\\x03" }, // tidak berefek di Co 3
  { "key": "zero",  "serial_cmd": "\\x02D\\x03" }, // tidak berefek di Co 3
  { "key": "print", "serial_cmd": "" }             // belum tervalidasi
]
```

### Aturan bridge

Aturan `bridge[]` opsional menyalin nilai yang dibaca dari satu device ke setpoint
di device lain, tiap poll cycle. Write bridge melewati control gate (otomatis,
bukan digerakkan operator).

```jsonc
"bridge": [
  {
    "read_from": { "device": "slave1", "key": "temp_out_chiller" },
    "write_to":  { "device": "plc1",   "key": "set_temp_tank" },
    "transform": "passthrough"
  }
]
```

## Tata letak topic MQTT

Semua topic diawali `{base_topic}/{location}/{name}` (machine id):

| Topic               | Arah      | Payload                                         |
| ------------------- | --------- | ----------------------------------------------- |
| `.../{param.topic}` | publish   | String skalar mentah, satu per param monitoring |
| `.../heartbeat`     | publish   | `{"status":"connected"}` tiap 1s (retained)     |
| `.../resources`     | publish   | `{"memory_pct","cpu_pct","ip"}` tiap 1s         |
| `.../plc/status`    | publish   | `{"status":"connected"\|"disconnected"}`        |
| `.../control/{key}` | subscribe | `"1"`/`"0"`/`"true"`/`"false"` → write aktuator |
| `.../set/{key}`     | subscribe | String numerik → write setpoint                 |
| `.../cmd/{key}`     | subscribe | Payload apa pun → kirim `serial_cmd` weigher    |

Last-Will-Testament broker mem-publish `{"status":"disconnected"}` (retained) ke
topic heartbeat mesin primer kalau agent mati atau network putus. Setelah broker
reconnect, agent otomatis subscribe ulang (rumqttc pakai clean session).

## Control gate

Write dari `control/{key}` (aktuator FC5/FC6) diotorisasi oleh hash Valkey/Redis
sebelum tiap write. Write `set/{key}` dan bridge melewati gate.

Write di-grant **hanya jika** flag global dan flag per-mesin sama-sama `"1"`:

```sh
redis-cli HSET control global 1
redis-cli HSET control acme/site/area2/machine_c 1
```

Kalau `CACHE_URL` kosong atau Valkey tidak terjangkau, **semua control di-deny**
(fail-safe). Nama field per-mesin adalah `{base_topic}/{machine_id}` — identifier
yang sama dengan prefix topic MQTT.

## Diagnosa timbangan (`scale`)

Sebelum menulis parser weigher, pakai subcommand bawaan untuk memeriksa output
serial mentah (baud rate benar, encoding, terminator baris):

```bash
edge-client scale                          # pilih port interaktif
edge-client scale --port COM3 --baud 4800  # port/baud tertentu
edge-client scale --port COM3 --scan       # coba baud rate umum (3s tiap-tiap)
edge-client scale --port COM3 --send "T\r\n"  # kirim tare sebelum baca
edge-client scale --list                   # tampilkan port serial lalu keluar
```

Tiap baris mencetak `HEX | ASCII-printable`. Teks terbaca berarti baud-nya benar;
titik (`.`) di posisi yang harusnya digit biasanya menandakan encoding non-ASCII
yang bisa diperbaiki dengan `byte_map` di mapping.

## Cross-compilation

`build.sh` mem-build `edge-client` untuk suatu target di dalam container dan
menaruh binary-nya ke `bin/`:

```bash
bash build.sh armv7-musl        # Raspberry Pi OS 32-bit, static (direkomendasikan)
bash build.sh armv7-glibc       # Raspberry Pi OS 32-bit, dynamic glibc
bash build.sh aarch64           # Raspberry Pi OS 64-bit / ARM64
bash build.sh x86_64            # Linux x86_64, dynamic glibc
bash build.sh x86_64-musl       # Linux x86_64, static
bash build.sh win-x86_64-gnu    # Windows x86_64 (mingw-w64)
bash build.sh win-x86_64-msvc   # Windows x86_64 (MSVC ABI via cargo-xwin)
```

Jalankan `bash build.sh` tanpa argumen untuk melihat semua target.

## Lisensi

Dilisensikan di bawah [MIT License](LICENSE).
