"""
将 china_city.geojson 中的省直辖县级单位（如济源、神农架、天门等）
合并进 china_district.geojson。

这类要素的特征：
  - level == "district"（区县级）
  - 但因直挂省级，出现在了市级文件中
  - adcode 前两位不属于四个直辖市（京11 津12 沪31 渝50）

用法：python merge_direct_districts.py
"""

import json
from pathlib import Path

# ══════════════════════════════════════════
#  配置区
# ══════════════════════════════════════════
CITY_FILE     = Path("./china/china_city.geojson")
DISTRICT_FILE = Path("./china/china_district.geojson")
OUTPUT_FILE   = Path("./china/china_district.geojson")  # 默认覆盖原文件，如需保留原文件可改名

DIRECT_CITY_PREFIXES = {"11", "12", "31", "50"}  # 京津沪渝，其区不在处理范围内
# ══════════════════════════════════════════


def get_adcode(props: dict) -> str:
    return str(props.get("adcode") or props.get("code") or props.get("id") or "").strip()


def is_province_direct_unit(props: dict) -> bool:
    """
    省直辖县级单位判断：
      level == "district" 且 adcode 前两位不属于直辖市
    """
    code  = get_adcode(props)
    level = props.get("level", "")
    return (
        len(code) == 6
        and level == "district"
        and code[:2] not in DIRECT_CITY_PREFIXES
    )


def main():
    print(f"读取 {CITY_FILE} ...")
    with open(CITY_FILE, encoding="utf-8") as f:
        city_data = json.load(f)

    print(f"读取 {DISTRICT_FILE} ...")
    with open(DISTRICT_FILE, encoding="utf-8") as f:
        district_data = json.load(f)

    # 提取省直辖县级单位
    province_direct = [
        feat for feat in city_data["features"]
        if is_province_direct_unit(feat["properties"])
    ]
    print(f"\n从市级文件中识别到 {len(province_direct)} 个省直辖县级单位：")
    for feat in province_direct:
        p = feat["properties"]
        print(f"  {p.get('name')}  adcode={get_adcode(p)}")

    if not province_direct:
        print("\n未找到省直辖县级单位，请检查 level 字段值是否为 'district'。")
        return

    # 去重：跳过区县文件中已存在的 adcode
    existing_codes = {get_adcode(f["properties"]) for f in district_data["features"]}
    to_add  = [f for f in province_direct if get_adcode(f["properties"]) not in existing_codes]
    skipped = len(province_direct) - len(to_add)
    if skipped:
        print(f"\n其中 {skipped} 个 adcode 已存在于区县文件，跳过。")

    # 合并并按 adcode 排序
    merged_features = to_add + district_data["features"]
    merged_features.sort(key=lambda f: get_adcode(f["properties"]))

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": merged_features},
                  f, ensure_ascii=False)

    print(f"\n完成！")
    print(f"  新增省直辖县级单位：{len(to_add)} 个")
    print(f"  原有区县要素：      {len(district_data['features'])} 个")
    print(f"  合并后总计：        {len(merged_features)} 个")
    print(f"  输出文件：{OUTPUT_FILE.resolve()}")


if __name__ == "__main__":
    main()