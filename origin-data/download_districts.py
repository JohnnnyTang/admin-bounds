"""
全国区县级行政区划批量下载脚本
数据源：阿里云 DataV GeoJSON API
用法：
  1. 将你的市级 geojson 路径填入 CITY_GEOJSON
  2. python download_districts.py
"""

import requests
import json
import time
from pathlib import Path

# ══════════════════════════════════════════
#  配置区（按需修改）
# ══════════════════════════════════════════
CITY_GEOJSON  = "./china/china_city.geojson"                      # 你的市级 geojson 路径
OUTPUT_DIR    = Path("./cache")           # 每市单独缓存，断点续传用
OUTPUT_MERGED = Path("./china/china_district.geojson")    # 最终合并输出
BASE_URL      = "https://geo.datav.aliyun.com/areas_v3/bound/geojson?code={code}_full"
DELAY_SEC     = 0.4          # 请求间隔（秒），避免触发限流
MAX_RETRIES   = 3            # 单个请求最大重试次数
# ══════════════════════════════════════════

OUTPUT_DIR.mkdir(exist_ok=True)
 
 
def load_city_codes(path: str) -> list[tuple[str, str]]:
    """
    从市级 geojson 提取所有市级 adcode。
    兼容 DataV 的 adcode / code / id 字段，以及 level 字段标注。
    直辖市（京津沪渝）的市辖区直接挂在省级代码下，此处一并处理。
    """
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
 
    DIRECT_PROVINCES = {"110000", "120000", "310000", "500000"}  # 京津沪渝，区已在市级，跳过
    codes = []
 
    for feat in data.get("features", []):
        props = feat.get("properties", {})
        code  = str(props.get("adcode") or props.get("code") or props.get("id") or "").strip()
        name  = props.get("name", "未知")
        level = props.get("level", "")
 
        if len(code) != 6:
            continue
        if code in DIRECT_PROVINCES:
            continue
 
        is_city = (
            (code[4:] == "00" and code[2:] != "0000")   # 标准地级市
            or level in ("city", "市级", "地级市")
        )
        if is_city:
            codes.append((code, name))
 
    return codes
 
 
def fetch_one(city_code: str, city_name: str) -> tuple[dict | None, bool]:
    """
    下载单个市的区县 geojson。
    优先读本地缓存（支持断点续传）。
    返回 (data, from_cache)。
    """
    cache = OUTPUT_DIR / f"{city_code}.geojson"
    if cache.exists():
        with open(cache, encoding="utf-8") as f:
            return json.load(f), True
 
    url = BASE_URL.format(code=city_code)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            data = r.json()
            with open(cache, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            return data, False
        except Exception as exc:
            wait = 1.5 * attempt
            print(f"    ↳ 第{attempt}次重试 [{city_name}] {exc}，等待 {wait:.1f}s")
            time.sleep(wait)
 
    return None, False
 
 
def main():
    print(f"读取市级数据：{CITY_GEOJSON}")
    city_list = load_city_codes(CITY_GEOJSON)
    if not city_list:
        print("未能解析出任何市级代码，请检查 geojson 文件的 properties 字段名。")
        return
    print(f"共识别到 {len(city_list)} 个市级行政区\n")
 
    all_features: list[dict] = []
    failed: list[tuple[str, str]] = []
    total = len(city_list)
 
    for i, (code, name) in enumerate(city_list, 1):
        data, cached = fetch_one(code, name)
        prefix = f"[{i:3d}/{total}]"
 
        if data and "features" in data:
            n = len(data["features"])
            all_features.extend(data["features"])
            src = "缓存" if cached else "下载"
            print(f"{prefix} [{src}] {name}（{code}）→ {n} 个区县，累计 {len(all_features)}")
        else:
            failed.append((code, name))
            print(f"{prefix} [失败] {name}（{code}）")
 
        if not cached:
            time.sleep(DELAY_SEC)
 
    # ── 合并写出 ──────────────────────────────────
    result = {
        "type": "FeatureCollection",
        "features": all_features,
    }
    with open(OUTPUT_MERGED, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False)
 
    print(f"\n{'─'*50}")
    print(f"完成！共 {len(all_features)} 个区县要素")
    print(f"输出文件：{OUTPUT_MERGED.resolve()}")
 
    if failed:
        print(f"\n以下 {len(failed)} 个市请求失败，可手动检查或重新运行（缓存已有的不会重复下载）：")
        for code, name in failed:
            print(f"  {name}（{code}）  {BASE_URL.format(code=code)}")
 
 
if __name__ == "__main__":
    main()