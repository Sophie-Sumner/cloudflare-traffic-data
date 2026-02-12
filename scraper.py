#!/usr/bin/env python3
"""
简洁版 Cloudflare Radar 数据下载器
支持按 ASN 或地区下载整年数据，使用 7 天窗口分包下载
"""

import json
import os
import time
import random
import csv
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
import multiprocessing
import requests
from multiprocessing import Pool

os.environ['CF_API_TOKEN'] = 'hj75JbccMpFyY9lmNmOYREmDRQ8Mmh79PreMhggq'

# 官方 Radar API
RADAR_API_BASE = "https://api.cloudflare.com/client/v4/radar"
CF_API_TOKEN_ENV = "CF_API_TOKEN"

# 全局限流控制
_RATE_LIMIT_LOCK = multiprocessing.Lock()
_LAST_REQUEST_TS = multiprocessing.Value("d", 0.0)


def rate_limited_request(interval: float = 1.0) -> None:
    """全局请求限流"""
    if interval <= 0:
        return

    with _RATE_LIMIT_LOCK:
        now = time.time()
        last = _LAST_REQUEST_TS.value

        if last > 0:
            sleep_time = max(0, last + interval - now)
            if sleep_time > 0:
                time.sleep(sleep_time)

        _LAST_REQUEST_TS.value = time.time()


def make_request(url: str, params: dict, headers: dict,
                 max_retries: int = 3, interval: float = 1.0) -> Dict[str, Any]:
    """带限流和重试的请求"""
    for attempt in range(max_retries):
        try:
            rate_limited_request(interval)

            resp = requests.get(url, params=params, headers=headers, timeout=30)

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", interval * 2))
                print(f"  限流等待 {retry_after} 秒...")
                time.sleep(retry_after)
                continue

            resp.raise_for_status()
            return resp.json()

        except Exception as e:
            if attempt < max_retries - 1:
                wait = (attempt + 1) * 2
                print(f"  请求失败，{wait}秒后重试: {e}")
                time.sleep(wait)
            else:
                raise

    raise Exception(f"请求失败，已重试 {max_retries} 次")


def get_api_token() -> str:
    """获取 API Token"""
    token = os.getenv(CF_API_TOKEN_ENV)
    if not token:
        raise ValueError(f"请设置环境变量 {CF_API_TOKEN_ENV}")
    return token


def generate_7day_windows(year: int) -> List[tuple]:
    """生成一年的 7 天时间窗口"""
    start = datetime(year, 1, 1)
    end = datetime(year, 12, 31, 23, 59, 59)

    windows = []
    current = start

    while current < end:
        window_end = current + timedelta(days=7)
        if window_end > end:
            window_end = end
        windows.append((current, window_end))
        current = window_end

    return windows


def fetch_window_data(target_type: str, target_id: Any,
                      start_date: datetime, end_date: datetime,
                      products: List[str] = None,
                      interval: str = "15m",
                      request_interval: float = 1.0) -> Dict[str, Any]:
    """下载一个时间窗口的数据"""
    if products is None:
        products = ["HTTP", "ALL"]

    token = get_api_token()
    url = f"{RADAR_API_BASE}/netflows/timeseries"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    all_data = {}
    date_start = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    date_end = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    for product in products:
        params = {
            "name": "main",
            "product": product,
            "dateStart": date_start,
            "dateEnd": date_end,
            "aggInterval": interval,
            "format": "json",
            "normalization": "MIN0_MAX",
        }

        if target_type == "asn":
            params["asn"] = target_id
        else:  # location
            params["location"] = target_id

        print(f"  下载 {target_id} {product} ({start_date.date()}~{end_date.date()})")

        try:
            data = make_request(url, params, headers, interval=request_interval)
            all_data[product] = data
        except Exception as e:
            print(f"    ✗ 失败: {e}")
            all_data[product] = None

    return all_data


def has_traffic(data: Dict[str, Any]) -> bool:
    """检查数据是否有流量"""
    try:
        values = data["result"]["main"].get("values", [])
        return any(float(v) != 0.0 for v in values if v is not None)
    except:
        return False


def process_target(args: tuple) -> None:
    """处理单个目标（ASN 或地区）的多进程任务"""
    target_type, target_id, windows, interval, request_interval, output_dir, year = args

    # 创建输出目录
    if target_type == "asn":
        base_dir = os.path.join(output_dir, "AS", f"as{target_id}")
    else:
        base_dir = os.path.join(output_dir, "country", f"loc_{target_id}")

    os.makedirs(base_dir, exist_ok=True)

    # 先测试一天是否有流量
    test_start, test_end = windows[0][0], windows[0][0] + timedelta(days=1)
    test_data = fetch_window_data(
        target_type, target_id, test_start, test_end,
        request_interval=request_interval
    )

    if not any(data and has_traffic(data) for data in test_data.values()):
        print(f"[{target_id}] 无流量，跳过")
        return

    print(f"[{target_id}] 开始下载全年数据...")

    # 初始化 CSV 文件
    csv_files = {}
    for product in ["HTTP", "ALL"]:
        csv_path = os.path.join(base_dir, f"radar_{year}_{product.lower()}.csv")
        if os.path.exists(csv_path):
            os.remove(csv_path)
        csv_files[product] = csv_path

    # 下载每个窗口的数据
    total_windows = len(windows)
    for i, (start_date, end_date) in enumerate(windows, 1):
        print(f"  [{i}/{total_windows}] {start_date.date()}~{end_date.date()}")

        window_data = fetch_window_data(
            target_type, target_id, start_date, end_date,
            request_interval=request_interval
        )

        # 保存到 CSV
        for product, data in window_data.items():
            if not data:
                continue

            try:
                series = data["result"]["main"]
                timestamps = series["timestamps"]
                values = series["values"]

                # 格式化时间戳
                formatted_times = [
                    datetime.fromisoformat(t.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M")
                    for t in timestamps
                ]

                # 写入 CSV
                csv_path = csv_files[product]
                file_exists = os.path.exists(csv_path)

                with open(csv_path, "a", encoding="utf-8", newline="") as f:
                    writer = csv.writer(f)
                    if not file_exists:
                        writer.writerow(["timestamp", "value"])
                    for t, v in zip(formatted_times, values):
                        writer.writerow([t, v])

            except Exception as e:
                print(f"    解析 {product} 数据失败: {e}")

    print(f"[{target_id}] 下载完成")


def download_year_data(
        asn_list: List[int] = None,
        locations: List[str] = None,
        year: int = 2025,
        interval: str = "15m",
        request_interval: float = 1.0,
        output_dir: str = "data",
        max_workers: int = 1
) -> None:
    """下载整年数据的主函数"""

    if not asn_list and not locations:
        print("错误：未指定 ASN 或地区")
        return

    # 生成时间窗口
    windows = generate_7day_windows(year)
    print(f"{year} 年共有 {len(windows)} 个 7 天窗口")

    # 准备任务
    tasks = []

    if asn_list:
        for asn in asn_list:
            tasks.append(("asn", asn, windows, interval, request_interval, output_dir, year))

    if locations:
        for loc in locations:
            tasks.append(("location", loc, windows, interval, request_interval, output_dir, year))

    print(f"共有 {len(tasks)} 个下载任务")

    # 执行下载
    if max_workers <= 1 or len(tasks) <= 1:
        # 单进程
        for task in tasks:
            process_target(task)
    else:
        # 多进程
        with Pool(processes=min(max_workers, len(tasks))) as pool:
            pool.map(process_target, tasks)


def load_csv_list(file_path: str, column: str) -> list:
    """从 CSV 加载列表"""
    if not os.path.exists(file_path):
        print(f"警告：文件不存在 {file_path}")
        return []

    items = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if column in row and row[column]:
                    items.append(row[column].strip())
    except Exception as e:
        print(f"读取 {file_path} 失败: {e}")

    return items


def main():

    """主函数"""
    print("=" * 60)
    print("Cloudflare Radar 数据下载器")
    print("=" * 60)

    # 配置
    base_dir = os.path.dirname(os.path.abspath(__file__))

    # 加载 ASN 列表
    asn_csv = os.path.join(base_dir, "data", "asns_all.csv")
    asn_list = [int(a) for a in load_csv_list(asn_csv, "asn") if a.isdigit()]

    # 加载地区列表
    loc_csv = os.path.join(base_dir, "data", "geolocations_countries.csv")
    locations = load_csv_list(loc_csv, "country_iso2")

    # 如果没有数据，使用示例
    if not asn_list and not locations:
        asn_list = [7303, 13335]  # 示例 ASN
        locations = ["US", "CN"]  # 示例地区

    print(f"ASN 数量: {len(asn_list)}")
    print(f"地区数量: {len(locations)}")

    # 下载配置
    config = {
        "asn_list": asn_list,
        "locations": locations,
        "year": 2025,
        "interval": "15m",
        "request_interval": 4.0,  # 请求间隔，建议 ≥ 2.0 秒
        "output_dir": "data",
        "max_workers": 4,  # 并发进程数
    }

    # 开始下载
    try:
        download_year_data(**config)
        print("\n" + "=" * 60)
        print("下载完成！")
        print("=" * 60)
    except Exception as e:
        print(f"\n下载失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
