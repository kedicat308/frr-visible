#!/bin/sh
# box-init.sh — router:v2 的 ENTRYPOINT。像真设备开机那样先拉起日志子系统,
# 再把控制权交给容器 CMD(build-topo 传的 sleep infinity)。
# 之后 config-l3vpn.sh 起 FRR、deploy-shim.sh 配 log syslog,日志自然汇入。
set -e

COLLECTOR="${COLLECTOR:-172.31.0.40}"   # promtail syslog 接收器(frr-mgmt IP)

# 注入收集器地址(就地改标准路径 /etc/rsyslog.conf,apparmor 只放行这个;幂等)
sed -i "s/__COLLECTOR__/${COLLECTOR}/" /etc/rsyslog.conf
mkdir -p /var/spool/rsyslog

# 1) 设备日志子系统:rsyslog(读默认 /etc/rsyslog.conf,建 /dev/log,转发 → 收集器)
rsyslogd

# 2) L2/kernel 事件 → syslog(本 netns 的 netlink,像真交换机报 link/FDB 变化)
( ip -o monitor link addr neigh route 2>/dev/null \
    | while IFS= read -r l; do logger -t netlink -p daemon.info -- "$l"; done ) &
( bridge monitor 2>/dev/null \
    | while IFS= read -r l; do logger -t bridge -p daemon.info -- "$l"; done ) &

logger -t box-init -p daemon.notice "device logging subsystem up (collector=${COLLECTOR})"

exec "$@"
