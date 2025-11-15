#!/usr/bin/env python3
"""
generate_uuid_mapping.py

根据 A.txt/B.txt 与 A/B 目录中的图片，通过图像相似度建立 uuid 映射（A_uuid -> B_uuid）。

方法概述：
- 使用差分哈希(dHash)作为主要特征（对小变动鲁棒）。
- 计算每张图片的 dHash（64-bit），并使用颜色直方图作为辅助判别。
- 以最小 Hamming 距离为优先，做贪心一对一匹配；若哈希距离不满足阈值，尝试使用直方图的 L1 距离作为回退。

依赖: Pillow, numpy

示例:
    python generate_uuid_mapping.py --a-list A.txt --b-list B.txt --a-dir A --b-dir B --out uuid_mapping.py

输出: 生成文件（默认 uuid_mapping.py），文件内包含 uuid_mapping 字典
"""
import argparse
import os
from typing import Dict, List, Tuple

import numpy as np
from PIL import Image


def read_list(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip()]
    # 支持行内为 {uuid}.webp 或者只为 uuid
    uuids = []
    for l in lines:
        name = os.path.basename(l)
        if name.lower().endswith(".webp"):
            uuids.append(name[:-5])
        else:
            uuids.append(name)
    return uuids


def dhash(image: Image.Image, hash_size: int = 8) -> int:
    """Compute difference hash (dHash) for an image, return as int (hash_size*hash_size bits).
    Reference: https://www.hackerfactor.com/blog/index.php?/archives/529-Kind-of-Like-That.html
    """
    # convert to grayscale and resize (hash_size+1, hash_size)
    image = image.convert("L").resize(
        (hash_size + 1, hash_size), Image.Resampling.LANCZOS
    )
    arr = np.asarray(image, dtype=np.uint8)
    # compute differences between adjacent columns
    diff = arr[:, 1:] > arr[:, :-1]
    # pack into integer
    bit_array = diff.flatten()
    h = 0
    for b in bit_array:
        h = (h << 1) | int(b)
    return h


def hamming_distance(a: int, b: int) -> int:
    return (a ^ b).bit_count()


def color_histogram(image: Image.Image, bins_per_channel: int = 8) -> np.ndarray:
    """Compute normalized RGB histogram (flattened).
    Return L1-normalized 1D array.
    """
    # ensure RGB
    im = image.convert("RGB")
    arr = np.asarray(im)
    # compute histogram per channel
    hist = []
    for ch in range(3):
        h, _ = np.histogram(arr[..., ch], bins=bins_per_channel, range=(0, 256))
        hist.append(h)
    hist = np.concatenate(hist).astype(np.float32)
    s = hist.sum()
    if s > 0:
        hist /= s
    return hist


def load_image_metrics(path: str) -> Tuple[int, np.ndarray]:
    with Image.open(path) as im:
        dh = dhash(im)
        ch = color_histogram(im)
    return dh, ch


def build_metrics_map(
    dir_path: str, uuids: List[str], ext: str = ".webp"
) -> Dict[str, Tuple[int, np.ndarray]]:
    m = {}
    for u in uuids:
        p = os.path.join(dir_path, u + ext)
        if not os.path.exists(p):
            raise FileNotFoundError(f"Image not found: {p}")
        m[u] = load_image_metrics(p)
    return m


def greedy_match(
    a_keys: List[str],
    b_keys: List[str],
    a_metrics: Dict[str, Tuple[int, np.ndarray]],
    b_metrics: Dict[str, Tuple[int, np.ndarray]],
    max_hamming: int = 10,
    hist_threshold: float = 0.35,
) -> Dict[str, str]:
    # build all pairwise hamming distances
    pairs = []  # (dist, a, b)
    for a in a_keys:
        ah, _ = a_metrics[a]
        for b in b_keys:
            bh, _ = b_metrics[b]
            d = hamming_distance(ah, bh)
            pairs.append((d, a, b))
    pairs.sort(key=lambda x: x[0])

    assigned_a = set()
    assigned_b = set()
    mapping = {}

    # first pass: assign by small hamming distances
    for d, a, b in pairs:
        if a in assigned_a or b in assigned_b:
            continue
        if d <= max_hamming:
            mapping[a] = b
            assigned_a.add(a)
            assigned_b.add(b)

    # second pass: for unassigned a, try histogram fallback
    remaining_a = [a for a in a_keys if a not in assigned_a]
    remaining_b = [b for b in b_keys if b not in assigned_b]
    if remaining_a and remaining_b:
        # precompute hist vectors
        for a in remaining_a:
            ah, a_hist = a_metrics[a]
            best_b = None
            best_score = float("inf")
            for b in remaining_b:
                bh, b_hist = b_metrics[b]
                # L1 distance between normalized histograms
                score = float(np.sum(np.abs(a_hist - b_hist)))
                if score < best_score:
                    best_score = score
                    best_b = b
            if best_b is not None and best_score <= hist_threshold:
                mapping[a] = best_b
                assigned_a.add(a)
                assigned_b.add(best_b)
                remaining_b.remove(best_b)

    return mapping


def save_mapping_py(mapping: Dict[str, str], out_path: str):
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("uuid_mapping = {\n")
        for a, b in mapping.items():
            f.write(f'"{a}": "{b}",\n')
        f.write("}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Generate uuid mapping between two image folders based on image similarity"
    )
    parser.add_argument(
        "--a-list", required=True, help="A.txt (lines contain {uuid}.webp or uuid)"
    )
    parser.add_argument("--b-list", required=True, help="B.txt")
    parser.add_argument("--a-dir", required=True, help="Directory for A images")
    parser.add_argument("--b-dir", required=True, help="Directory for B images")
    parser.add_argument(
        "--out", default="uuid_mapping.py", help="Output mapping python file"
    )
    parser.add_argument(
        "--ext", default=".webp", help="Image extension (default .webp)"
    )
    parser.add_argument(
        "--max-hamming",
        type=int,
        default=10,
        help="Max allowed hamming distance for direct hash match (default 10)",
    )
    parser.add_argument(
        "--hist-threshold",
        type=float,
        default=0.35,
        help="Max L1 histogram distance to accept fallback match (default 0.35)",
    )
    args = parser.parse_args()

    a_uuids = read_list(args.a_list)
    b_uuids = read_list(args.b_list)

    print(f"Found {len(a_uuids)} entries in A list and {len(b_uuids)} in B list")

    a_metrics = build_metrics_map(args.a_dir, a_uuids, ext=args.ext)
    b_metrics = build_metrics_map(args.b_dir, b_uuids, ext=args.ext)

    mapping = greedy_match(
        a_uuids,
        b_uuids,
        a_metrics,
        b_metrics,
        max_hamming=args.max_hamming,
        hist_threshold=args.hist_threshold,
    )

    print(f"Matched {len(mapping)} pairs (out of {len(a_uuids)} A entries)")
    save_mapping_py(mapping, args.out)
    print(f"Wrote mapping to {args.out}")


if __name__ == "__main__":
    main()
