#!/usr/bin/env bash
# traceview.sh — 拉取某节点 shim 的 convergence trace(frr:/traces HTTP 端点)
# 并渲染成瀑布图。这是 §15.6「控制面收敛 trace」的查看工具:一次拓扑事件
# (链路/邻接变化)在 netlink / OSPF / FPM / BMP 各总线上引发的因果时间线。
#
# 用法:  traceview.sh <node-mgmt-ip> [all|last|<id>]     默认 all
#        traceview.sh 172.31.0.11            # pe1 的全部 trace
#        traceview.sh 172.31.0.11 last       # 只看最近一条
# 在 my-frr 虚机内运行(需能访问 172.31.0.0/24 的 :9340)。
set -uo pipefail

IP=${1:-172.31.0.11}
SEL=${2:-all}
J=$(curl -s --max-time 5 "http://$IP:9340/traces" 2>/dev/null) || { echo "连接失败 $IP:9340"; exit 1; }
[ -z "$J" ] && { echo "无数据 ($IP:9340)"; exit 1; }
n=$(echo "$J" | jq 'length' 2>/dev/null) || { echo "响应非 JSON"; exit 1; }
[ "$n" = 0 ] && { echo "(该节点暂无 convergence trace——制造一次 flap 试试)"; exit 0; }

case "$SEL" in
  all)  filt='.[]' ;;
  last) filt=".[$((n-1))]" ;;
  *)    filt=".[] | select(.id==$SEL)" ;;
esac

echo "$J" | jq -r "$filt |
  \"TRACE\t\(.node)\t\(.id)\t\(.root)\t\(.span_ms)\t\(.start)\",
  (.spans[] | \"SPAN\t\(.off_ms)\t\(.bus)/\(.kind)\t\(.key)\t\(.detail)\")" \
| awk -F'\t' '
  $1=="TRACE"{ printf "\n== convergence trace @ %s  #%s ==\n   root: %s\n   span=%sms  start=%s\n", $2,$3,$4,$5,$6 }
  $1=="SPAN"{ printf "   +%5sms  %-20s %-16s %s\n", $2,$3,$4,$5 }'
