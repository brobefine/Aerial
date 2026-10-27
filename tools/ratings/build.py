"""
Сборщик готовых оценок (27 октября): IMDb, Rotten Tomatoes (критики и зрители), Metacritic, Letterboxd — без ключа у пользователя.

Каждый запуск дополняет уже собранное (папка вывода — прошлое содержимое ветки ratings-data):
- очередь — все фильмы и сериалы TMDb по популярности (ежедневные выгрузки files.tmdb.org, без ключа);
- MDBList (MDBLIST_API_KEY, ~1000 запросов в день) — всё сразу, самым популярным; часть квоты — на обновление старых;
- OMDb (OMDB_API_KEY, ~1000 в день) — следующим по популярности: IMDb, RT критиков, Metacritic (номер IMDb — у TMDb);
- IMDb — ежедневный файл datasets.imdbws.com: свежая оценка всем, у кого известен номер IMDb, и ещё до IMDB_ONLY
  работ из очереди сверх квот (номер IMDb у TMDb, лимита нет).
Нет ключа — этот источник пропускается.

Вывод: <папка>/imdb/<номер IMDb без tt // 10000>.json — весь файл IMDb, {"76759": 8.6}: приложение берёт оценку IMDb
любой работы по её номеру IMDb (номер — у TMDb), даже если работы ещё нет в собранном;
<папка>/movie/<id // 1000>.json и <папка>/tv/<id // 1000>.json, в каждом {"<id TMDb>": [imdb, tomatoes, popcorn,
metacritic, letterboxd, imdbId, год, день сбора, источник]}; null — нет оценки. Приложению нужны первые пять — шкалы как
у MDBList (`MdbListRatings.formatValues`): IMDb 0–10, RT и Metacritic 0–100, Letterboxd 0–5. День — с 1 января 2020,
источник: m — MDBList (полно), o — OMDb (без зрителей RT и Letterboxd), i — только IMDb.

Запуск: python build.py <папка>. Ключи — из окружения: MDBLIST_API_KEY, OMDB_API_KEY, TMDB_API_KEY (или TMDB_READ_TOKEN).
"""
import concurrent.futures as cf
import datetime
import gzip
import io
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

MDB_KEY = os.environ.get("MDBLIST_API_KEY", "").strip()
OMDB_KEY = os.environ.get("OMDB_API_KEY", "").strip()
TMDB_KEY = os.environ.get("TMDB_API_KEY", "").strip()
TMDB_TOKEN = os.environ.get("TMDB_READ_TOKEN", "").strip()

MDB_BUDGET = int(os.environ.get("MDB_BUDGET", "950"))
OMDB_BUDGET = int(os.environ.get("OMDB_BUDGET", "950"))
IMDB_ONLY = int(os.environ.get("IMDB_ONLY", "5000"))
QUEUE = int(os.environ.get("QUEUE", "400000"))  # дальше по популярности — почти пустые карточки
REFRESH_SHARE = 0.3  # доля квоты MDBList на обновление уже собранного
FRESH_DAYS, OLD_DAYS = 14, 180  # как часто обновлять: работы этого и прошлого года / остальные

EPOCH = datetime.date(2020, 1, 1)
TODAY = (datetime.date.today() - EPOCH).days
YEAR = datetime.date.today().year
I_IMDB, I_TOMATOES, I_POPCORN, I_META, I_LBX, I_IMDB_ID, I_YEAR, I_DAY, I_SRC = range(9)


def get(url, headers=None, tries=3):
    req = urllib.request.Request(url, headers={"User-Agent": "aerial-ratings", **(headers or {})})
    for i in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code in (401, 403, 404, 429) or i == tries - 1:
                raise
        except Exception:
            if i == tries - 1:
                raise
        time.sleep(1.5 * (i + 1))


def num(v, lo, hi):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return round(x, 1) if lo <= x <= hi else None


# ---------- очередь ----------

def export(kind):
    """Все работы TMDb вида kind (movie / tv_series) с популярностью: выгрузка за вчера или раньше."""
    for back in range(1, 5):
        d = datetime.date.today() - datetime.timedelta(days=back)
        url = f"https://files.tmdb.org/p/exports/{kind}_ids_{d:%m_%d_%Y}.json.gz"
        try:
            raw = gzip.decompress(get(url))
        except Exception:
            continue
        rows = []
        for line in raw.splitlines():
            r = json.loads(line)
            if not r.get("adult") and not r.get("video"):
                rows.append((r.get("popularity") or 0.0, r["id"]))
        print(f"{kind}: {len(rows)} из выгрузки {d}")
        return rows
    raise SystemExit(f"нет выгрузки TMDb {kind}")


def queue():
    rows = [(p, "movie", i) for p, i in export("movie")] + [(p, "tv", i) for p, i in export("tv_series")]
    rows.sort(reverse=True)
    return [(t, i) for _, t, i in rows[:QUEUE]]


# ---------- хранилище ----------

class Store:
    def __init__(self, root):
        self.root, self.shards, self.dirty = root, {}, set()

    def shard(self, kind, tmdb):
        key = (kind, tmdb // 1000)
        if key not in self.shards:
            path = os.path.join(self.root, kind, f"{tmdb // 1000}.json")
            self.shards[key] = json.load(open(path, encoding="utf-8")) if os.path.exists(path) else {}
        return self.shards[key]

    def get(self, kind, tmdb):
        return self.shard(kind, tmdb).get(str(tmdb))

    def put(self, kind, tmdb, row):
        self.shard(kind, tmdb)[str(tmdb)] = row
        self.dirty.add((kind, tmdb // 1000))

    def load_all(self):
        for kind in ("movie", "tv"):
            folder = os.path.join(self.root, kind)
            if os.path.isdir(folder):
                for name in os.listdir(folder):
                    if name.endswith(".json"):
                        self.shard(kind, int(name[:-5]) * 1000)

    def rows(self):
        for (kind, _), shard in self.shards.items():
            for tmdb, row in shard.items():
                yield kind, int(tmdb), row

    def save(self):
        for kind, n in self.dirty:
            os.makedirs(os.path.join(self.root, kind), exist_ok=True)
            with open(os.path.join(self.root, kind, f"{n}.json"), "w", encoding="utf-8") as f:
                json.dump(self.shards[(kind, n)], f, separators=(",", ":"), sort_keys=True)
        print(f"записано файлов: {len(self.dirty)}")


def due(row):
    age = TODAY - row[I_DAY]
    return age >= (FRESH_DAYS if (row[I_YEAR] or 0) >= YEAR - 1 else OLD_DAYS)


# ---------- источники ----------

def mdblist(kind, tmdb):
    t = "show" if kind == "tv" else "movie"
    body = json.loads(get(f"https://api.mdblist.com/tmdb/{t}/{tmdb}/?apikey={urllib.parse.quote(MDB_KEY)}"))
    if str((body.get("ids") or {}).get("tmdb")) != str(tmdb):
        return None
    vals = {r.get("source"): r.get("value") for r in body.get("ratings") or []}
    row = [num(vals.get("imdb"), 0, 10), num(vals.get("tomatoes"), 0, 100), num(vals.get("popcorn"), 0, 100),
           num(vals.get("metacritic"), 0, 100), num(vals.get("letterboxd"), 0, 5),
           (body.get("ids") or {}).get("imdb"), body.get("year"), TODAY, "m"]
    return row


def tmdb_imdb(kind, tmdb):
    q = {"api_key": TMDB_KEY} if TMDB_KEY else {}
    headers = {"Authorization": "Bearer " + TMDB_TOKEN} if TMDB_TOKEN and not TMDB_KEY else {}
    try:
        body = json.loads(get(f"https://api.themoviedb.org/3/{kind}/{tmdb}/external_ids?" + urllib.parse.urlencode(q), headers))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    return body.get("imdb_id") or None


def omdb(imdb_id):
    body = json.loads(get(f"https://www.omdbapi.com/?i={imdb_id}&apikey={urllib.parse.quote(OMDB_KEY)}"))
    if body.get("Response") != "True":
        if "limit" in (body.get("Error") or "").lower():
            raise urllib.error.HTTPError("", 429, "limit", None, None)
        return None
    vals = {r.get("Source"): r.get("Value", "") for r in body.get("Ratings") or []}
    rt = vals.get("Rotten Tomatoes", "").rstrip("%")
    mc = vals.get("Metacritic", "").split("/")[0]
    year = (body.get("Year") or "")[:4]
    return [num(body.get("imdbRating"), 0, 10), num(rt, 0, 100), None, num(mc, 0, 100), None,
            imdb_id, int(year) if year.isdigit() else None, TODAY]


def imdb_dataset():
    raw = gzip.decompress(get("https://datasets.imdbws.com/title.ratings.tsv.gz"))
    out = {}
    for line in io.TextIOWrapper(io.BytesIO(raw), encoding="utf-8"):
        parts = line.rstrip("\n").split("\t")
        if len(parts) == 3 and parts[0].startswith("tt"):
            value = num(parts[1], 0, 10)
            if value is not None and parts[0][2:].isdigit():
                out[parts[0]] = value
    print(f"IMDb: {len(out)} оценок")
    return out


def write_imdb(root, ratings):
    """Весь файл IMDb кусками по номеру: каждый раз заново (оценки меняются у всех)."""
    shards = {}
    for imdb_id, value in ratings.items():
        n = int(imdb_id[2:])
        shards.setdefault(n // 10000, {})[str(n)] = value
    folder = os.path.join(root, "imdb")
    if os.path.isdir(folder):
        for name in os.listdir(folder):
            os.remove(os.path.join(folder, name))
    os.makedirs(folder, exist_ok=True)
    for k, shard in shards.items():
        with open(os.path.join(folder, f"{k}.json"), "w", encoding="utf-8") as f:
            json.dump(shard, f, separators=(",", ":"), sort_keys=True)
    print(f"IMDb: файлов {len(shards)}")


# ---------- сборка ----------

def main():
    root = sys.argv[1] if len(sys.argv) > 1 else "ratings"
    store = Store(root)
    store.load_all()
    order = queue()
    rank = {w: n for n, w in enumerate(order)}
    fresh = [w for w in order if store.get(*w) is None]
    print(f"собрано: {len(order) - len(fresh)} из {len(order)}; нового в очереди: {len(fresh)}")
    taken = set()

    if MDB_KEY:
        refresh = sorted((rank.get((k, i), 1 << 30), k, i) for k, i, row in store.rows()
                         if (k, i) in rank and (row[I_SRC] != "m" or due(row)))
        plan = [(k, i) for _, k, i in refresh[:int(MDB_BUDGET * REFRESH_SHARE)]]
        plan += fresh[:MDB_BUDGET - len(plan)]
        if len(plan) < MDB_BUDGET:  # нового и устаревшего мало — обновляем дальше по популярности
            plan += [(k, i) for _, k, i in refresh[len(plan):MDB_BUDGET]]
        got = 0
        for kind, tmdb in plan:
            try:
                row = mdblist(kind, tmdb)
            except urllib.error.HTTPError as e:
                if e.code in (401, 403, 429):
                    print(f"MDBList: {e.code}, остановка")
                    break
                row = None
            except Exception:
                row = None
            taken.add((kind, tmdb))
            if row:
                store.put(kind, tmdb, row)
                got += 1
            time.sleep(0.2)
        print(f"MDBList: {got} из {len(plan)}")

    rest = [w for w in fresh if w not in taken]
    ratings = imdb_dataset()
    write_imdb(root, ratings)
    if OMDB_KEY or IMDB_ONLY:
        omdb_left, got_o, got_i = OMDB_BUDGET if OMDB_KEY else 0, 0, 0
        plan = rest[:omdb_left + IMDB_ONLY] if (TMDB_KEY or TMDB_TOKEN) else []
        with cf.ThreadPoolExecutor(8) as pool:
            ids = list(pool.map(lambda w: tmdb_imdb(*w), plan))
        for (kind, tmdb), imdb_id in zip(plan, ids):
            if not imdb_id:
                continue
            row = None
            if omdb_left > 0:
                omdb_left -= 1
                try:
                    row = omdb(imdb_id)
                except urllib.error.HTTPError:
                    print("OMDb: лимит, остановка")
                    omdb_left = 0
                except Exception:
                    pass
                if row:
                    row.append("o")
                    got_o += 1
            if not row and imdb_id in ratings:
                row = [ratings[imdb_id], None, None, None, None, imdb_id, None, TODAY, "i"]
                got_i += 1
            if row:
                store.put(kind, tmdb, row)
        print(f"OMDb: {got_o}; только IMDb: {got_i}")

    # Свежая оценка IMDb всем собранным — бесплатно, из файла.
    updated = 0
    for kind, tmdb, row in list(store.rows()):
        value = ratings.get(row[I_IMDB_ID] or "")
        if value is not None and value != row[I_IMDB]:
            row[I_IMDB] = value
            store.put(kind, tmdb, row)
            updated += 1
    print(f"IMDb обновлено: {updated}")

    store.save()
    total = sum(1 for _ in store.rows())
    with open(os.path.join(root, "info.json"), "w", encoding="utf-8") as f:
        json.dump({"day": TODAY, "count": total, "queue": len(order)}, f)
    print(f"всего в базе: {total}")


if __name__ == "__main__":
    main()
