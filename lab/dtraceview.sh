#!/usr/bin/env bash
# dtraceview.sh — 渲染跨设备「分布式收敛 trace」(trace-aggregator 的 :9341/dtraces)。
# 把各节点各自的 convergence trace 按时间聚类、合并成一条端到端时间线,每个 span
# 标注来源节点;若是链路事件,标出规范化的链路两端(pe1-p1 ↔ p1-pe1)。见 design.md §15.7。
#
# 用法:  dtraceview.sh <aggregator-ip> [all|last|<id>]     默认 all
#        dtraceview.sh 172.31.0.32
set -uo pipefail

IP=${1:-172.31.0.32}
SEL=${2:-all}
J=$(curl -s --max-time 5 "http://$IP:9341/dtraces" 2>/dev/null) || { echo "连接失败 $IP:9341"; exit 1; }
[ -z "$J" ] && { echo "无数据"; exit 1; }
n=$(echo "$J" | jq 'length' 2>/dev/null) || { echo "响应非 JSON"; exit 1; }
[ "$n" = 0 ] && { echo "(暂无分布式 trace——制造一次 flap 试试)"; exit 0; }

case "$SEL" in
  all)  f='.[]' ;;
  last) f=".[$((n-1))]" ;;
  *)    f=".[] | select(.id==$SEL)" ;;
esac

echo "$J" | jq -r "$f |
  \"DT\t\(.id)\t\(.link // \"-\")\t\(.span_ms)\t\((.nodes|join(\",\")))\t\(.start)\",
  (.spans[] | \"SP\t\(.off_ms)\t\(.node)\t\(.bus)/\(.kind)\t\(.key)\t\(.detail)\")" \
| awk -F'\t' '
  $1=="DT"{ printf "\n== distributed convergence trace #%s ==\n   link=%s  span=%sms  nodes=[%s]\n   start=%s\n", $2,$3,$4,$5,$6 }
  $1=="SP"{ printf "   +%6sms  %-4s %-20s %-16s %s\n", $2,$3,$4,$5,$6 }'
