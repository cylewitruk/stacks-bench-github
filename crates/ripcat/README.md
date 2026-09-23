# ripcat

`ripcat` fetches parallel HTTP byte ranges and writes one ordered output stream.
It is intended for large compressed objects whose consumer must remain
sequential.

```text
HTTP ranges -> bounded disk-backed reorder window -> ordered stdout
```

Each range is bound to the strong ETag observed by the initial probe. Transient
range failures reconnect at the first missing byte. Completed chunks wait in a
private temporary directory and are removed after they reach stdout, so memory
use and temporary disk use remain bounded by the configured window.

```bash
ripcat --connections 8 --window-mib 512 \
  https://example.test/archive.tar.zst \
  | zstd --decompress --stdout \
  | tar --extract --file -
```

The tool can retry individual range failures without restarting its consumer.
It cannot resume an interrupted process because it deliberately does not retain
the full compressed object.

Progress on stderr reports ordered output speed and ETA from the latest 60
one-second samples, or fewer during startup. It prints at most once every five
seconds. The line includes a readable transferred/total amount and exact byte
counts, plus the currently active chunk downloads. Samples without completed
chunks lower the speed during a stall.
