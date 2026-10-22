"""
Сборщик готовых стен «Вселенных» (22 октября).

Для каждой вселенной и языка — картинка мозаики `<key>.<lang>.webp` (1728×1008, растр стены приложения один в один:
72×28 клеток 24×36, поле 1.5 px, скругление 4 px) и список `<key>.<lang>.json` (2016 клеток по порядку, null — пусто).
Приложение режет картинку на 8 блоков и пропускает 2000 загрузок постеров (`WallStore.remote` в VodWall.kt).

Запросы — те же, что `WallUniverse` в VodTmdbClient.kt. Порядок клеток — как `VodWall.order`: самые известные в центре.
Ключ TMDb — из окружения: TMDB_API_KEY (v3) или TMDB_READ_TOKEN (v4). Запуск: python build.py <папка вывода>.
"""
import concurrent.futures as cf
import io
import json
import os
import sys
import time
import urllib.parse
import urllib.request

from PIL import Image, ImageDraw

COLS, ROWS, CELL_W, CELL_H = 72, 28, 24, 36
COUNT = COLS * ROWS
INSET, RADIUS = 1.5, 4.0
PLACEHOLDER = (0x1B, 0x1E, 0x24, 255)
SS = 4  # сглаживание: клетка рисуется вчетверо крупнее и ужимается

UNIVERSES = {
    "movies": ("movie", {"sort_by": "vote_count.desc", "without_genres": "16"}),
    "series": ("tv", {"sort_by": "vote_count.desc", "without_genres": "16"}),
    "cartoons": ("movie", {"sort_by": "vote_count.desc", "with_genres": "16"}),
    "cartoon-series": ("tv", {"sort_by": "vote_count.desc", "with_genres": "16"}),
}
LANGS = {"ru": "ru-RU", "en": "en-US"}

KEY = os.environ.get("TMDB_API_KEY", "").strip()
TOKEN = os.environ.get("TMDB_READ_TOKEN", "").strip()


def get(url, auth=False, tries=4):
    req = urllib.request.Request(url, headers={"User-Agent": "aerial-universes"})
    if auth and TOKEN and not KEY:
        req.add_header("Authorization", "Bearer " + TOKEN)
    for i in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read()
        except Exception:
            if i == tries - 1:
                raise
            time.sleep(1.5 * (i + 1))


def api(path, params):
    q = dict(params, include_adult="false")
    if KEY:
        q["api_key"] = KEY
    return json.loads(get("https://api.themoviedb.org/3/" + path + "?" + urllib.parse.urlencode(q), auth=True))


def order():
    # Целые вместо дробей: (c % COLS - 35.5) * 24 — точно то же, что Float в приложении, без расхождений на равных.
    def d(c):
        dx = (2 * (c % COLS) - (COLS - 1)) * CELL_W
        dy = (2 * (c // COLS) - (ROWS - 1)) * CELL_H
        return dx * dx + dy * dy
    return sorted(range(COUNT), key=d)


def films(kind, params, lang):
    genres = {g["id"]: g["name"] for g in api(f"genre/{kind}/list", {"language": lang})["genres"]}
    out, seen, page = [], set(), 1
    while len(out) < COUNT and page <= 500:
        rows = api(f"discover/{kind}", dict(params, language=lang, page=str(page)))["results"]
        if not rows:
            break
        for r in rows:
            if r.get("adult") or not r.get("poster_path") or r["id"] in seen:
                continue
            seen.add(r["id"])
            title = r.get("title" if kind == "movie" else "name") or ""
            if not title:
                continue
            date = r.get("release_date" if kind == "movie" else "first_air_date") or ""
            vote = r.get("vote_average") or 0
            out.append({
                "id": f"tmdb:{'movie' if kind == 'movie' else 'series'}:{r['id']}", "remote": str(r["id"]), "title": title,
                "poster": "https://image.tmdb.org/t/p/w500" + r["poster_path"],
                "backdrop": ("https://image.tmdb.org/t/p/w1280" + r["backdrop_path"]) if r.get("backdrop_path") else "https://image.tmdb.org/t/p/w500" + r["poster_path"],
                "year": date[:4], "genre": " / ".join(genres[g] for g in r.get("genre_ids", []) if g in genres),
                "rating": f"TMDb {vote:.1f}/10" if vote > 0 else "TMDb —", "about": r.get("overview") or "",
            })
            if len(out) >= COUNT:
                break
        page += 1
    return out


def rounded_mask(w, h):
    m = Image.new("L", (w * SS, h * SS), 0)
    ImageDraw.Draw(m).rounded_rectangle([0, 0, w * SS - 1, h * SS - 1], radius=RADIUS * SS, fill=255)
    return m.resize((w, h), Image.LANCZOS)


def build(key, kind, params, lang_code, lang, out_dir):
    items = films(kind, params, lang)
    cells = [None] * COUNT
    for film, cell in zip(items, order()):
        cells[cell] = film
    atlas = Image.new("RGBA", (COLS * CELL_W, ROWS * CELL_H), (0, 0, 0, 0))
    w, h = int(CELL_W - 2 * INSET), int(CELL_H - 2 * INSET)  # 21×33
    mask = rounded_mask(w, h)
    placeholder = Image.new("RGBA", (w, h), PLACEHOLDER)

    def poster(film):
        try:
            img = Image.open(io.BytesIO(get(film["poster"].replace("/w500/", "/w92/")))).convert("RGB")
        except Exception:
            return None
        s = max(w / img.width, h / img.height)  # по центру с обрезкой, как BitmapShader в приложении
        img = img.resize((max(w, round(img.width * s)), max(h, round(img.height * s))), Image.LANCZOS)
        l, t = (img.width - w) // 2, (img.height - h) // 2
        return img.crop((l, t, l + w, t + h)).convert("RGBA")

    with cf.ThreadPoolExecutor(16) as pool:
        images = list(pool.map(lambda f: poster(f) if f else None, cells))
    for cell, img in enumerate(images):
        tile = img or placeholder
        # Поле 1.5 px округлено до 2: клетка 21×33 с левого верхнего угла (2, 2) — зазоры те же 3 px между постерами.
        layer = Image.new("RGBA", (w, h), (0, 0, 0, 0)); layer.paste(tile, (0, 0), mask)
        atlas.alpha_composite(layer, ((cell % COLS) * CELL_W + 2, (cell // COLS) * CELL_H + 2))
    atlas.save(os.path.join(out_dir, f"{key}.{lang_code}.webp"), "WEBP", quality=86, method=6)
    with open(os.path.join(out_dir, f"{key}.{lang_code}.json"), "w", encoding="utf-8") as f:
        json.dump({"format": 1, "films": cells}, f, ensure_ascii=False, separators=(",", ":"))
    print(f"{key}.{lang_code}: {sum(1 for c in cells if c)} films", flush=True)


def main():
    if not KEY and not TOKEN:
        sys.exit("нужен TMDB_API_KEY или TMDB_READ_TOKEN")
    out_dir = sys.argv[1] if len(sys.argv) > 1 else "universes"
    os.makedirs(out_dir, exist_ok=True)
    for key, (kind, params) in UNIVERSES.items():
        for code, lang in LANGS.items():
            build(key, kind, params, code, lang, out_dir)
    with open(os.path.join(out_dir, "index.json"), "w", encoding="utf-8") as f:
        json.dump({"format": 1, "built": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "universes": list(UNIVERSES)}, f)


if __name__ == "__main__":
    main()
