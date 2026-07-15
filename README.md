# frr-visible

> 📊 **可视化收尾报告 · 读写合体,四平面互证** → **https://kedicat308.github.io/frr-visible/**
> (治理面 frr-net 与本读平面在 SoT 缝合:txn↔span 贯线 · 遥测同源 · 读平面自愈 · EVPN 解析 · 前缀因果链)

**EN** — A **gNMI shim** that wraps an FRR container: event-driven ingesters feed FRR/kernel state into an OpenConfig cache, exposed over gNMI (Subscribe / Get / Capabilities). It makes an FRR box *look like* a gNMI-speaking router-switch without modifying FRR. Full design in [`design.md`](design.md).

**中** — 给 FRR 容器体外套一层 **gNMI 壳**:事件驱动 ingester 把 FRR/内核状态灌进一棵 OpenConfig cache,对外用 gNMI(Subscribe / Get / Capabilities)暴露。不改 FRR,让它"看起来像"会说 gNMI 的路由交换机。完整设计见 [`design.md`](design.md)。

## Architecture / 架构

```
FRR daemons ─ BMP (BGP/L3VPN) ──┐
            ─ FPM (routes/FIB) ─┤
kernel ─ netlink (if/vlan/fdb) ─┤→ ingesters → internal/state (openconfig/gnmi cache, "cache-centric")
       ─ cgroup (cpu/mem) ──────┤            → internal/gnmiserver (Subscribe + Get + Capabilities)
lldpd ─ lldpcli ────────────────┤            → gNMI client (gnmic, Telegraf, CloudVision, …)
ospfd ─ syslog ─────────────────┘
```

**EN** — Event-driven where the kernel/protocol pushes (netlink multicast, FPM, BMP, syslog, lldpd); SAMPLE only for true gauges (counters, CPU/mem) — same as commercial NOS. Three origins share one cache: `openconfig` (standard), `host` (container, not in OC), `frr` (L3VPN RD/RT/label OC covers poorly).

**中** — 凡是内核/协议主动推的就事件驱动(netlink 组播、FPM、BMP、syslog、lldpd);只有真正的 gauge(计数、CPU/内存)才 SAMPLE——和商用 NOS 一致。三个 origin 共用一棵 cache:`openconfig`(标准)、`host`(容器,OC 没有)、`frr`(L3VPN 的 RD/RT/label,OC 覆盖弱)。

## Metric coverage / 指标覆盖 (8/8)

| # | Metric / 指标 | Ingester | Path (origin) |
|---|------|----------|------|
| 1 | Container CPU/mem / 容器 CPU·内存 | cgroup | `host:/container/{cpu,memory}/state/*` |
| 2 | Port status/traffic / 端口状态·流量 | netlink | `openconfig:/interfaces/interface/state{,/counters}` |
| 3 | VLAN / FDB | netlink | `openconfig:/network-instances/.../fdb/mac-table` |
| 4 | MPLS FIB | fpm | `openconfig:/network-instances/.../afts` |
| 5 | OSPF neighbors / OSPF 邻居 | ospf (syslog) | `openconfig:/network-instances/.../ospfv2/.../neighbor` |
| 6 | BGP neighbors / BGP 邻居 | bmp | `openconfig:/network-instances/.../bgp/neighbors/neighbor` |
| 7 | LLDP | lldp | `openconfig:/lldp/interfaces/.../neighbor` |
| 8 | MPLS L3VPN | bmp + fpm | control: `frr:/bgp-rib/.../route[rd][prefix]` · forwarding: `openconfig:.../afts` |

**EN** — For one VPN route, control plane (BMP: RD/RT/label) and forwarding plane (FPM: VRF FIB) align in the same cache — the core value.
**中** — 同一条 VPN 路由,控制面(BMP:RD/RT/label)和转发面(FPM:VRF FIB)在同一棵 cache 对齐——核心价值。

## gNMI RPC coverage / gNMI RPC 覆盖

- ✅ **Subscribe** — streaming telemetry (STREAM/ONCE + ON_CHANGE/SAMPLE) / 流式遥测
- ✅ **Get** — one-shot snapshot, verified on all 3 origins with gnmic / 一次性快照,三 origin 均经 gnmic 实测
- ✅ **Capabilities** — model/encoding discovery / 模型·编码发现
- ⬜ **Set** — config push; the only remaining piece (write side, higher risk) / 配置下发,唯一剩余(写侧,风险高)

## Layout / 目录

- `cmd/frr-visible` — main program (cache + gNMI server + all ingesters) / 主程序
- `cmd/subtest` — tiny gNMI Subscribe test client (`-once`/STREAM, `-origin`, `-path`) / 验证客户端
- `internal/state` — OpenConfig cache wrapper / cache 封装
- `internal/gnmiserver` — gNMI server: Subscribe + Get + Capabilities / gNMI 服务端
- `internal/ingest/fpm.go` — routes / VRF FIB + nexthop-group parsing / 转发面
- `internal/ingest/bmp.go` — BGP peer state + VPNv4 routes (RD/RT/label) / 控制面
- `internal/ingest/netlink.go` — interface status (ON_CHANGE) / counters (SAMPLE) / FDB
- `internal/ingest/lldp.go` — lldpcli watch trigger + json reconcile + 15s fallback
- `internal/ingest/cgroup.go` — container CPU/memory (host origin, SAMPLE)
- `internal/ingest/ospf.go` — OSPF neighbors via syslog trigger + vtysh reconcile
- `internal/ingest/vrf.go` — VRF table-id → name (netlink)
- `lab/` — reproducible L3VPN test lab (pe1/pe2 configs + run scripts) / 可复现测试环境

## Build / Run / 构建·运行

Go 1.24+. Deploy either **embedded** (shim inside the FRR container — CPU/mem = FRR container, lldpcli/vtysh reachable) or **sidecar** (`--network container:<frr>`, shares netns).
构建需 Go 1.24+。部署可选**嵌入式**(shim 跑在 FRR 容器内——CPU/内存即 FRR 容器,lldpcli/vtysh 可达)或 **sidecar**(`--network container:<frr>`,共享 netns)。

```bash
CGO_ENABLED=0 go build -o /tmp/frr-visible ./cmd/frr-visible
CGO_ENABLED=0 go build -o /tmp/subtest     ./cmd/subtest

# embedded: run the shim inside the FRR container / 嵌入式:shim 跑进 FRR 容器
docker cp /tmp/frr-visible pe1:/shim
docker exec -d pe1 sh -c "/shim -gnmi :9339 -fpm 127.0.0.1:2620 -bmp 127.0.0.1:5000 -target frr"

# point FRR's FPM/BMP at the shim, enable OSPF syslog / 把 FRR 的 FPM/BMP 指向 shim,开 OSPF syslog
docker exec pe1 vtysh -c "conf t" -c "fpm address 127.0.0.1 port 2620" \
  -c "router bgp 65000" -c "bmp targets T1" -c "bmp connect 127.0.0.1 port 5000 min-retry 1000 max-retry 5000"
docker exec pe1 vtysh -c "conf t" -c "log syslog informational" -c "router ospf" -c "log-adjacency-changes detail"

# query with the real gnmic client / 用真实 gnmic 客户端查询
gnmic -a 172.30.0.11:9339 --insecure capabilities
gnmic -a 172.30.0.11:9339 --insecure get --path "openconfig:/interfaces/interface[name=eth0]/state/oper-status"
gnmic -a 172.30.0.11:9339 --insecure get --path "frr:/bgp-rib/afi-safis/afi-safi[name=l3vpn-ipv4-unicast]/routes"
```

Reproduce the L3VPN test lab / 复现 L3VPN 测试环境: see `lab/run2.sh` (OSPF+LDP underlay, iBGP VPNv4, cust VRF on each PE).

## Gotchas / 踩坑记录

- **⚠️ /dev/log back-pressure deadlock / 回压死锁 (important)** — Binding `/dev/log` (unix datagram, *reliable* delivery) as a syslog sink: if the reader is slow (e.g. inline `fork vtysh` per message), the receive buffer fills and FRR's `syslog()` **blocks**, wedging every daemon (vtysh hangs). **A monitor must never harm the monitored.** Fix: the syslog loop only *drains* + non-blocking signal; a separate debounced worker reconciles; `SetReadBuffer(1MB)`. Production: prefer `FRR log file` + inotify tail (the writer never blocks). / shim 绑 `/dev/log`(可靠投递)读得慢会阻塞 FRR 的 `syslog()` 拖垮 daemon。修复=解耦排空+去抖 worker+1MB 缓冲。生产建议改「log file + inotify」。
- **bind-mount inode trap / inode 坑** — `go build -o` makes a new inode; `-v file:/x` binds the old one, so the container runs the stale binary. Use `docker cp`. / 用 `docker cp` 更新容器内二进制。
- **lldpcli watch block-buffering / 块缓冲** — its stdout block-buffers over a pipe; a 15s periodic reconcile is the safety net. / 加 15s 周期兜底。
- **LLDP needs same mount ns as lldpd** (lldpcli uses a Unix socket). / LLDP 需与 lldpd 同 mount ns。

## Next / 下一步

- **Set** (config push) — turn the read-only monitor into a configurable target; start from a low-risk subset (L2 first, not BGP/OSPF core). / 配置下发,从低风险子集起步。
- Harden OSPF syslog to `log file` + inotify. / OSPF syslog 硬化为 log file + inotify。
- IPv6 / multi-nexthop / AF_MPLS LFIB / more AFT fields. / IPv6、多下一跳、AF_MPLS LFIB、更多 AFT 字段。
