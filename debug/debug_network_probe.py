# -*- coding: utf-8 -*-
import requests
import socket
import time
import sys

# 目标：平安银行 (000001) - 这是一个绝对存在的深市老票
HOST = "push2his.eastmoney.com"
URL = "http://push2his.eastmoney.com/api/qt/stock/kline/get"

def probe():
    print("========================================")
    print("🚀 开始 Docker 网络连通性深度诊断")
    print("========================================")

    # 1. DNS 解析测试
    print(f"\n[1/3] 测试 DNS 解析: {HOST}")
    try:
        ip = socket.gethostbyname(HOST)
        print(f"   ✅ 解析成功! IP: {ip}")
    except Exception as e:
        print(f"   ❌ 解析失败: {e}")
        print("   💡 诊断: Docker 无法解析域名。可能是宿主机 DNS 问题或 Docker 网络配置问题。")
        print("   🚑 建议: 重启 Docker (systemctl restart docker) 或修改 /etc/docker/daemon.json")
        return # DNS 挂了后面就不用测了

    # 2. 端口连通性测试 (TCP Ping)
    print(f"\n[2/3] 测试端口连通性 (TCP :80)")
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        result = s.connect_ex((ip, 80))
        if result == 0:
            print(f"   ✅ 连接成功!")
        else:
            print(f"   ❌ 连接失败 (错误码: {result})")
            print("   💡 诊断: 服务器防火墙拦截或对方服务器宕机。")
        s.close()
    except Exception as e:
        print(f"   ❌ 连接异常: {e}")

    # 3. API 接口测试 (HTTP GET)
    print(f"\n[3/3] 测试 K 线接口数据获取")
    params = {
        "secid": "0.000001", "klt": "101", "fqt": "1", "lmt": "10",
        "fields1": "f1", "fields2": "f51,f53"
    }
    headers = {"User-Agent": "Mozilla/5.0", "Connection": "close"}
    
    try:
        start = time.time()
        res = requests.get(URL, params=params, headers=headers, timeout=5)
        elapsed = (time.time() - start) * 1000
        
        print(f"   📡 状态码: {res.status_code}")
        print(f"   ⏱️ 耗时: {elapsed:.2f} ms")
        
        if res.status_code == 200:
            data = res.json()
            if data and data.get('data'):
                print(f"   ✅ 数据获取成功! 示例: {str(data['data']['klines'][:1])}")
            else:
                print(f"   ⚠️ 接口通了但无数据: {res.text[:100]}")
        else:
            print(f"   ❌ HTTP 错误。如果是 403/429/418，说明 IP 被封了。")
            
    except Exception as e:
        print(f"   ❌ 请求崩溃: {e}")

if __name__ == "__main__":
    probe()
