import requests
import time
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
import threading
from threading import Lock
import csv

# =====================
# 全局配置区（可根据需要修改）
# =====================
URL = "https://myip.thordata.com/v1/ipinfo"
PROXY_TEMPLATE = "rmmsg2sa.{as_value}.thordata.net:9999"
AUTH_TEMPLATE = "td-customer-GH43726-country-{af}:GH43726"
REGIONS = ["pr", "na", "eu", "as"]  # 代理区域
CONCURRENCY = 200 # 并发线程数
CONNECT_TIMEOUT = 10  # 连接超时时间(秒)
READ_TIMEOUT = 20     # 读取超时时间(秒)
BATCH_SIZE = 2000     # CSV批量写入条数
MONITOR_INTERVAL = 30  # 监控刷新间隔(秒)


# =====================
# 全局状态对象
# =====================
write_queue = Queue()
stop_event = threading.Event()
file_lock = Lock()
monitor_data = {
    "混播": {"count": 0, "latest": []},
    "美洲": {"count": 0, "latest": []},
    "欧洲": {"count": 0, "latest": []},
    "亚洲": {"count": 0, "latest": []},
}
monitor_lock = Lock()


# =====================
# 核心功能函数
# =====================
def writer_thread():
    """CSV写入线程"""
    batch_data = {}

    while not stop_event.is_set() or not write_queue.empty():
        try:
            sheet_name, result = write_queue.get(timeout=1)

            # 批量数据收集
            if sheet_name not in batch_data:
                batch_data[sheet_name] = []
            batch_data[sheet_name].append(result)

            # 批量写入条件（关键修复点）
            if len(batch_data[sheet_name]) >= BATCH_SIZE or (stop_event.is_set() and len(batch_data[sheet_name]) > 0):
                _write_csv_batch(sheet_name, batch_data[sheet_name])

                # 更新监控数据
                with monitor_lock:
                    monitor_data[sheet_name]["count"] += len(batch_data[sheet_name])
                    monitor_data[sheet_name]["latest"] = (
                            batch_data[sheet_name][-5:] +
                            monitor_data[sheet_name]["latest"][:5]
                    )

                batch_data[sheet_name] = []

        except Empty:   # 静默处理超时，不打印错误
            continue
        except Exception as e:
            print(f"写入线程异常: {str(e)}")

    # 强制写入残留数据（关键修复点）
    for sheet_name in list(batch_data.keys()):
        if len(batch_data[sheet_name]) > 0:
            _write_csv_batch(sheet_name, batch_data[sheet_name])
            with monitor_lock:
                if sheet_name in monitor_data:
                    monitor_data[sheet_name]["count"] += len(batch_data[sheet_name])
                    monitor_data[sheet_name]["latest"] = (
                            batch_data[sheet_name][-5:] +
                            monitor_data[sheet_name]["latest"][:5]
                    )
            del batch_data[sheet_name]


def _write_csv_batch(sheet_name, data_batch):
    """执行CSV批量写入"""
    try:
        filename = f"{sheet_name}.csv"
        headers = ["请求国家", "大洲", "返回国家", "IP", "延迟"]

        with file_lock:
            # 检查文件状态
            file_exists = os.path.exists(filename)
            write_header = not file_exists or os.stat(filename).st_size == 0

            # 写入数据
            with open(filename, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                if write_header:
                    writer.writeheader()

                # 数据格式化
                formatted = [
                    {
                        "请求国家": item["请求国家"],
                        "大洲": item["大洲"],
                        "返回国家": item["返回国家"],
                        "IP": item["IP"],
                        "延迟": item["延迟"]
                    } for item in data_batch
                ]
                writer.writerows(formatted)
    except Exception as e:
        print(f"写入文件 {filename} 失败: {str(e)}")
        print(f"待写入数据量: {len(data_batch)}")
        raise



def monitor_thread():
    """实时监控线程"""
    while not stop_event.is_set():
        time.sleep(MONITOR_INTERVAL)
        with monitor_lock:
            total = sum(data["count"] for data in monitor_data.values())
            if total == 0:
                continue  # 无数据时不打印监控信息
            print("\n" + "=" * 50)
            print(f" 监控时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("-" * 50)
            for sheet, data in monitor_data.items():
                print(f"▶ {sheet}.csv")
                print(f"   总记录数: {data['count']:>8}")
                print(f"   最新延迟样本:")
                for i, record in enumerate(data['latest'][:3], 1):
                    delay = record["延迟"].center(12)
                    country = record["请求国家"].ljust(8)
                    print(f"     {i}. {delay} | 国家: {country}")
            print("=" * 50 + "\n")


def _make_request(url, region, guojia, proxy_template, auth_template, timeout):
    """执行单个请求（动态协议代理版本）"""
    try:
        # ======================
        # 关键修复1：协议自适应代理配置
        # ======================
        # 判断请求协议类型
        protocol = "https" if url.startswith("https://") else "http"

        # 生成完整代理地址（包含端口）
        proxy_host = proxy_template.format(as_value=region)

        # 解析认证信息（兼容密码含特殊字符的情况）
        auth_parts = auth_template.split(":", 1)  # 只分割一次
        auth_username = auth_parts[0].format(af=guojia)
        auth_password = auth_parts[1] if len(auth_parts) > 1 else ""

        # 构建代理配置（根据协议动态选择键）
        proxies = {
            protocol: f"http://{auth_username}:{auth_password}@{proxy_host}"
        }

        # 记录 HTTP 请求开始时间
        request_start_time = time.time()

        # 发起请求 (超时控制: 10s 连接超时，20s 读取超时，确保总时间不超过 30s)
        response = requests.get(url, proxies=proxies, timeout=(10, 20))

        # 记录请求结束时间
        request_end_time = time.time()

        # 计算请求耗时（精确计算请求从发起到成功的时间）
        elapsed = (request_end_time - request_start_time) * 1000  # ms

        # 处理响应
        if response.status_code == 200:
            data = response.json()
            return {
                "region": region,
                "请求国家": guojia,
                "大洲": data.get("continent", "N/A"),
                "返回国家": data.get("country", "N/A"),
                "IP": data.get("client_ip", "N/A"),
                "延迟": f"{elapsed:.2f} ms"  # 保持原来的字段名称
            }
        return {
            "region": region,
            "请求国家": guojia,
            "大洲": "N/A",
            "返回国家": "N/A",
            "IP": "N/A",
            "延迟": f"HTTP_{response.status_code}"
        }

    except requests.exceptions.Timeout:
        return {
            "region": region,
            "请求国家": guojia,
            "大洲": "N/A",
            "返回国家": "N/A",
            "IP": "N/A",
            "延迟": "Timeout"
        }
    except Exception as e:
        return {
            "region": region,
            "请求国家": guojia,
            "大洲": "N/A",
            "返回国家": "N/A",
            "IP": "N/A",
            "延迟": f"Error: {type(e).__name__}"
        }


def merge_to_excel():
    """合并CSV到Excel（原样保留所有数据版）"""
    print("\n开始合并CSV文件...")
    start = time.time()

    sheet_mapping = {
        "pr": "混播",
        "na": "美洲",
        "eu": "欧洲",
        "as": "亚洲"
    }

    with pd.ExcelWriter('最终报告.xlsx', engine='openpyxl') as writer:
        for region, sheet_name in sheet_mapping.items():
            csv_file = f"{sheet_name}.csv"
            if os.path.exists(csv_file):
                try:
                    # 读取CSV文件（完全保留原始数据）
                    full_df = pd.read_csv(
                        csv_file,
                        dtype="string",  # 所有列强制为字符串类型
                        keep_default_na=False,  # 禁止自动转换"NA"为NaN
                        encoding='utf-8-sig'
                    )

                    # 直接写入Excel，不做任何替换
                    full_df.to_excel(
                        writer,
                        sheet_name=sheet_name,
                        index=False,
                        na_rep=""  # 仅处理真正的空值（非"N/A"）
                    )
                    print(f"成功合并: {sheet_name}.csv ({len(full_df)}条)")
                except Exception as e:
                    print(f"合并失败 {csv_file}: {str(e)}")
                    if "full_df" in locals():
                        print(f"异常数据样例:\n{full_df.head()}")
            else:
                print(f"文件不存在: {csv_file}")

    print(f"合并完成，耗时: {time.time() - start:.2f}秒")


# =====================
# 主控制函数
# =====================
def fetch_url_with_timeout():
    """主请求函数"""
    # 读取国家数据
    try:
        df = pd.read_excel("country_pd.xlsx")
        guojia_values = df['Xc'].tolist() or []
        print(f"成功读取 {len(guojia_values)} 个国家数据")
    except Exception as e:
        print(f"读取错误: {e}")
        guojia_values = []

    # 启动工作线程
    writer = threading.Thread(target=writer_thread)
    monitor = threading.Thread(target=monitor_thread)
    writer.start()
    monitor.start()

    total_tasks = len(REGIONS) * len(guojia_values) * 1000
    start_time = time.time()

    try:
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
            futures = []

            # 提交任务
            for region in REGIONS:
                for guojia in guojia_values:
                    for _ in range(1000):
                        futures.append(
                            executor.submit(
                                _make_request,
                                URL, region, guojia,
                                PROXY_TEMPLATE, AUTH_TEMPLATE, (CONNECT_TIMEOUT, READ_TIMEOUT)  #  传入新的超时设置
                            )
                        )

            # 处理结果
            for i, future in enumerate(as_completed(futures), 1):
                result = future.result()

                # 进度显示
                if i % 100 == 0:
                    elapsed = time.time() - start_time
                    speed = i / elapsed
                    remain = (total_tasks - i) / speed if speed > 0 else 0
                    print(
                        f"\r进度: {i}/{total_tasks} | "
                        f"速度: {speed:.1f} req/s | "
                        f"剩余: {remain / 60:.1f} min",
                        end="", flush=True
                    )

                # 入队写入
                sheet_name = {
                    "pr": "混播",
                    "na": "美洲",
                    "eu": "欧洲",
                    "as": "亚洲"
                }.get(result["region"], "混播")
                write_queue.put((sheet_name, result))

    finally:
        # 清理资源
        stop_event.set()
        writer.join()
        monitor.join()

        # 检查队列残留（关键修复点）
        if not write_queue.empty():
            print(f"\n警告: 队列中残留 {write_queue.qsize()} 条数据未处理")

        merge_to_excel()

        # 最终统计
        total = sum(data["count"] for data in monitor_data.values())
        print(f"\n总处理请求: {total}")
        print(f"总耗时: {time.time() - start_time:.2f}秒")


# =====================
# 程序入口
# =====================
if __name__ == "__main__":
    fetch_url_with_timeout()
