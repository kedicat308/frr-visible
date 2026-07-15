#!/usr/bin/env bash
# setup-telemetry.sh — wire the 5 shims' gnmic-frr collector into the existing
# Prometheus + Grafana stack. Run inside the my-frr VM after deploy-shim.sh.
set -euo pipefail

TELEM=/home/fanwei.guest/arista/telemetry
LAB=/Users/fanwei/arista/frr-visible/lab

# 1. gnmic-frr collector — dual-homed: frr-mgmt (reach the 8 shim :9339) +
#    campus-mgmt (so Prometheus can scrape :9806). Recreate to pick up new targets.
docker rm -f gnmic-frr >/dev/null 2>&1 || true
docker run -d --name gnmic-frr --network frr-mgmt --ip 172.31.0.30 \
  -v "$LAB/gnmic-frr.yaml:/app/gnmic.yaml:ro" --restart unless-stopped \
  ghcr.io/openconfig/gnmic:latest --config /app/gnmic.yaml subscribe >/dev/null
docker network connect campus-mgmt gnmic-frr --ip 172.30.30.20
echo "gnmic-frr started (frr-mgmt 172.31.0.30 + campus-mgmt 172.30.30.20)"

# 1b. pathtrace-exporter — gNMI-native path trace as Prometheus metrics. Runs the
#     same walk as pathtrace-gnmi.sh for configured flows; dual-homed like gnmic-frr
#     (frr-mgmt to reach the shims, campus-mgmt so Prometheus can scrape :9808).
SRC=/Users/fanwei/arista/frr-visible
INV="pe1=172.31.0.11,pe2=172.31.0.12,p1=172.31.0.21,p2=172.31.0.22,ce1=172.31.0.101,ce2=172.31.0.102,ce3=172.31.0.103,ce4=172.31.0.104"
FLOWS="${PATHTRACE_FLOWS:-ce1-ce4:ce1>10.255.1.4,ce4-ce1:ce4>10.255.1.1,ce2-ce3:ce2>10.255.1.3}"
( cd "$SRC" && CGO_ENABLED=0 GOFLAGS=-buildvcs=false go build -o /tmp/pathtrace-exporter ./cmd/pathtrace-exporter )
docker rm -f pathtrace-exporter >/dev/null 2>&1 || true
docker run -d --name pathtrace-exporter --network frr-mgmt --ip 172.31.0.31 \
  -v /tmp/pathtrace-exporter:/pathtrace-exporter:ro \
  -e INVENTORY="$INV" -e FLOWS="$FLOWS" -e INTERVAL=15s -e LISTEN=":9808" \
  --entrypoint /pathtrace-exporter --restart unless-stopped \
  ghcr.io/openconfig/gnmic:latest >/dev/null
docker network connect campus-mgmt pathtrace-exporter --ip 172.30.30.21
echo "pathtrace-exporter started (frr-mgmt 172.31.0.31 + campus-mgmt 172.30.30.21); flows: $FLOWS"

# 1c. Tempo — trace backend for the cross-device convergence traces (Zipkin +
#     OTLP receivers, local storage). Grafana queries it via the Tempo datasource.
docker rm -f tempo >/dev/null 2>&1 || true
docker run -d --name tempo --network campus-mgmt --user 0 --restart unless-stopped \
  -v "$LAB/tempo.yaml:/etc/tempo.yaml:ro" grafana/tempo:latest -config.file=/etc/tempo.yaml >/dev/null
cat > "$TELEM/grafana/provisioning/datasources/tempo.yml" <<'EOF'
apiVersion: 1
datasources:
  - name: Tempo
    type: tempo
    uid: tempo
    access: proxy
    url: http://tempo:3200
    editable: true
    jsonData:
      # trace → 日志 联动:点开某跳 span → 跳到那台设备(span 的 service.name)
      # 在该时段(±2min)的 Loki syslog。共同键=设备名(host),不靠 traceID。
      tracesToLogsV2:
        datasourceUid: loki
        spanStartTimeShift: '-2m'
        spanEndTimeShift: '2m'
        filterByTraceID: false
        filterBySpanID: false
        tags:
          - key: service.name
            value: host
EOF
echo "tempo started (campus-mgmt) + Grafana Tempo datasource provisioned"

# 1d. trace-aggregator — stitch each shim's :9340/traces into cross-device
#     distributed traces and export them to Tempo (Zipkin). Dual-homed: frr-mgmt
#     to pull the shims, campus-mgmt to reach Tempo. See design.md §15.7/§15.8.
( cd "$SRC" && CGO_ENABLED=0 GOFLAGS=-buildvcs=false go build -o /tmp/trace-aggregator ./cmd/trace-aggregator )
docker rm -f trace-aggregator >/dev/null 2>&1 || true
docker run -d --name trace-aggregator --network frr-mgmt --ip 172.31.0.32 \
  -v /tmp/trace-aggregator:/trace-aggregator:ro \
  -e INVENTORY="$INV" -e WINDOW=1.5s -e INTERVAL=3s -e LISTEN=":9341" \
  -e TEMPO_ZIPKIN="http://tempo:9411/api/v2/spans" \
  --entrypoint /trace-aggregator --restart unless-stopped \
  ghcr.io/openconfig/gnmic:latest >/dev/null
docker network connect campus-mgmt trace-aggregator --ip 172.30.30.22
echo "trace-aggregator started (frr-mgmt 172.31.0.32 + campus-mgmt 172.30.30.22 -> Tempo)"

# 1e. topology-exporter — reads each shim's gNMI directly and emits the 4-layer
#     topology (physical/ospf/bgp/mpls) as Node-Graph-ready Prometheus metrics.
#     Dual-homed like the others (frr-mgmt to reach shims, campus-mgmt to scrape).
( cd "$SRC" && CGO_ENABLED=0 GOFLAGS=-buildvcs=false go build -o /tmp/topology-exporter ./cmd/topology-exporter )
docker rm -f topology-exporter >/dev/null 2>&1 || true
docker run -d --name topology-exporter --network frr-mgmt --ip 172.31.0.33 \
  -v /tmp/topology-exporter:/topology-exporter:ro \
  -e INVENTORY="$INV" -e INTERVAL=15s -e LISTEN=":9810" \
  --entrypoint /topology-exporter --restart unless-stopped \
  ghcr.io/openconfig/gnmic:latest >/dev/null
docker network connect campus-mgmt topology-exporter --ip 172.30.30.23
echo "topology-exporter started (frr-mgmt 172.31.0.33 + campus-mgmt 172.30.30.23)"

# 1f. on-change 订阅健康:只报警,不自动重启(避免重启本身成为抖动源)。
#     BGP/OSPF/FIB 走 on-change(保事件精度/能抓 flap);采集器订阅一旦僵死,
#     这些序列会消失而 sample 的接口计数器仍在。判据做成看板上的红绿灯 stat
#     面板(id 7 "on-change 遥测健康",见 frr-visible-dashboard.json,由下面
#     section 3 provision):STALE/0 时手动 `docker restart gnmic-frr` 即可。
#     刻意不做自动重启容器(曾验证:重启会造成全指标秒级缺口、且易震荡)。

# 1g. 日志支柱:Loki(存)+ promtail(Docker 服务发现,自动 tail 所有容器 stdout)。
#     补齐 metrics/traces/logs 三支柱;也作 frr-net 治理面审计日志的 sink。
#     注意:clab 的 FRR 车队(pe1..ce4)daemon stdout 不连 PID1,其自身日志需另采
#     (log file + bind,或 shim 暴露),不在此;此处收全栈容器 + clab 节点日志。
docker pull grafana/loki:latest >/dev/null 2>&1 || true
docker pull grafana/promtail:latest >/dev/null 2>&1 || true
docker rm -f loki >/dev/null 2>&1 || true
docker run -d --name loki --network campus-mgmt \
  -v "$LAB/loki.yaml:/etc/loki/loki.yaml:ro" -v loki-data:/loki \
  --restart unless-stopped grafana/loki:latest -config.file=/etc/loki/loki.yaml >/dev/null
docker rm -f promtail >/dev/null 2>&1 || true
docker run -d --name promtail --network campus-mgmt \
  -v "$LAB/promtail.yaml:/etc/promtail/promtail.yaml:ro" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  --restart unless-stopped grafana/promtail:latest -config.file=/etc/promtail/promtail.yaml >/dev/null
# 双归 frr-mgmt 172.31.0.40:接收 router:v2 车队的设备级 remote syslog(:1514)
docker network connect frr-mgmt promtail --ip 172.31.0.40 2>/dev/null || true
echo "loki + promtail started (campus-mgmt + frr-mgmt 172.31.0.40; stdout + device syslog -> Loki)"
# Grafana Loki 数据源
cat > "$TELEM/grafana/provisioning/datasources/loki.yml" <<'EOF'
apiVersion: 1
datasources:
  - name: Loki
    type: loki
    uid: loki
    access: proxy
    url: http://loki:3100
    editable: true
EOF
echo "Loki datasource provisioned"

# 2. Prometheus scrape job for gnmic-frr (idempotent)
if ! grep -q "gnmic-frr" "$TELEM/prometheus.yml"; then
  cat >> "$TELEM/prometheus.yml" <<'EOF'

  # 5-node FRR+shim fleet via gnmic-frr (frr-visible)
  - job_name: gnmic-frr
    static_configs:
      - targets: ['gnmic-frr:9806']
EOF
  echo "added gnmic-frr scrape job"
else
  echo "prometheus scrape job already present"
fi
if ! grep -q "pathtrace-exporter" "$TELEM/prometheus.yml"; then
  cat >> "$TELEM/prometheus.yml" <<'EOF'

  # gNMI-native path trace (frr-visible)
  - job_name: pathtrace-exporter
    static_configs:
      - targets: ['pathtrace-exporter:9808']
EOF
  echo "added pathtrace-exporter scrape job"
fi
if ! grep -q "topology-exporter" "$TELEM/prometheus.yml"; then
  cat >> "$TELEM/prometheus.yml" <<'EOF'

  # multi-layer topology (frr-visible)
  - job_name: topology-exporter
    static_configs:
      - targets: ['topology-exporter:9810']
EOF
  echo "added topology-exporter scrape job"
fi

# 3. Provision the dashboard into Grafana
cp "$LAB/frr-visible-dashboard.json" "$TELEM/grafana/provisioning/dashboards/frr-visible.json"
echo "dashboard provisioned"

# 3b. Provision Grafana 统一告警规则(接口/OSPF/BGP/VPN/MPLS-LFIB + 遥测健康/采集器)
#     通知走内置默认 = 仅在 Grafana 告警页/看板 Alert list 显示,不外发。
mkdir -p "$TELEM/grafana/provisioning/alerting"
cp "$LAB/alert-rules.yaml" "$TELEM/grafana/provisioning/alerting/alert-rules.yaml"
echo "alert rules provisioned"

# 4. (Re)start Prometheus so it reloads the config; nudge Grafana too
docker start prometheus >/dev/null 2>&1 || true
docker restart prometheus >/dev/null 2>&1 || true
docker restart grafana   >/dev/null 2>&1 || true

echo "== done =="
echo "   Grafana:    http://localhost:3000  (dashboard: FRR-visible; Explore -> Tempo for traces)"
echo "   Prometheus: http://localhost:9090"
echo "   Traces:     path metrics on the dashboard's Trace row; convergence traces in Tempo"
