import io
import json
import os
import chess.pgn
import psycopg2
from psycopg2.extras import execute_values
from fastapi import FastAPI, UploadFile, File, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List

app = FastAPI(title="Victory Chess 3+1 Family Cluster API (Serverless Ready)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 450 MB Safe Limit (Supabase 500MB မကျော်စေရန် Buffer 50MB ချန်ထားသည်)
MAX_DB_BYTES = 450 * 1024 * 1024
HARD_VACUUM_BYTES = 485 * 1024 * 1024
MANIFEST_FILE = "cluster_manifest.json"

# 🌟 Offline (favorite_db.dart) နှင့် တစ်ထပ်တည်းကျစေရန် ပထမဆုံးအပိုင်း (Board Piece Placement) ကိုသာ သုံးမည်
INITIAL_BOARD_FEN = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR"

# ၁။ FEN Cluster Group အတွက် (Project 1 - Region: ap-northeast-1)
FEN_CLUSTER_URLS = [
    "postgresql://postgres.lzeqykackqajwfaqpzgq:2006ChessMandalay@aws-0-ap-northeast-1.pooler.supabase.com:5432/postgres",
]

# ၂။ Service Clusters Group အတွက် (Project 2 - Region: ap-southeast-1)
SERVER_CLUSTERS = [
    {
        "game_data": "postgresql://postgres.czekgqvmpugtydfxvlcu:2006ChessMandalay@aws-0-ap-southeast-1.pooler.supabase.com:5432/postgres",
        "raw_moves": "postgresql://postgres.czekgqvmpugtydfxvlcu:2006ChessMandalay@aws-0-ap-southeast-1.pooler.supabase.com:5432/postgres",
        "fen_map":   "postgresql://postgres.lzeqykackqajwfaqpzgq:2006ChessMandalay@aws-0-ap-northeast-1.pooler.supabase.com:5432/postgres",
    },
]

class GamePayload(BaseModel):
    white: str
    white_elo: Optional[int] = None
    black: str
    black_elo: Optional[int] = None
    result: str = "?-?"
    event: str = ""
    site: str = ""
    round: str = ""
    eco: str = ""
    game_date: str = ""
    moves_text: str

def get_clean_fen(board) -> str:
    """Offline favorite_db.dart ကဲ့သို့ FEN ၏ ပထမဆုံးအပိုင်း (အရုပ်အနေအထား) ကိုသာ ယူမည်"""
    return board.fen().split(" ")[0]

def clean_fen_str(fen_str: str) -> str:
    """ရှာဖွေလာသော FEN String မှ ပထမဆုံးအပိုင်း (အရုပ်အနေအထား) ကိုသာ သန့်စင်ထုတ်ယူမည်"""
    parts = fen_str.strip().split()
    return parts[0] if parts else ""

def extract_fens_from_end_to_start(game) -> List[str]:
    board = game.board()
    fens_in_order = []
    for move in game.mainline_moves():
        board.push(move)
        c_fen = get_clean_fen(board)
        if c_fen and c_fen != INITIAL_BOARD_FEN:
            fens_in_order.append(c_fen)

    return list(dict.fromkeys(reversed(fens_in_order)))

def get_db_size(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT pg_database_size(current_database());")
        return cur.fetchone()[0]

def vacuum_if_needed(conn):
    if get_db_size(conn) >= HARD_VACUUM_BYTES:
        old_autocommit = conn.autocommit
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("VACUUM;")
        conn.autocommit = old_autocommit

def init_tables_for_3in1(conn_game, conn_moves, conn_map):
    with conn_game.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS game_data (
                game_id INTEGER PRIMARY KEY,
                white TEXT, white_elo INTEGER,
                black TEXT, black_elo INTEGER,
                result VARCHAR(10), event TEXT, site TEXT,
                round VARCHAR(20), eco VARCHAR(10), game_date VARCHAR(20)
            );
            CREATE INDEX IF NOT EXISTS idx_white ON game_data(white);
            CREATE INDEX IF NOT EXISTS idx_black ON game_data(black);
            CREATE INDEX IF NOT EXISTS idx_event ON game_data(event);
        """)
    conn_game.commit()

    with conn_moves.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_moves (
                game_id INTEGER PRIMARY KEY,
                moves_text TEXT
            );
        """)
    conn_moves.commit()

    with conn_map.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fen_game_map (
                fen_id INTEGER PRIMARY KEY,
                game_ids INTEGER[]
            );
        """)
    conn_map.commit()

def init_table_for_fen(conn_fen):
    with conn_fen.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fen_positions (
                fen_id INTEGER PRIMARY KEY,
                fen_string TEXT UNIQUE
            );
            CREATE INDEX IF NOT EXISTS idx_fen_string ON fen_positions(fen_string);
        """)
    conn_fen.commit()

def _default_manifest():
    return {
        "active_fen_cluster_idx": 0,
        "active_3in1_cluster_idx": 0,
        "last_game_id": 0,
        "last_fen_id": 0,
        "fen_clusters": {},
        "3in1_clusters": {},
        "moved_games_registry": {}
    }

def _normalize_manifest(data: dict) -> dict:
    data.setdefault("active_fen_cluster_idx", 0)
    data.setdefault("active_3in1_cluster_idx", 0)
    data.setdefault("last_game_id", 0)
    data.setdefault("last_fen_id", 0)
    data.setdefault("fen_clusters", {})
    data.setdefault("3in1_clusters", {})
    data.setdefault("moved_games_registry", {})
    return data

def load_or_init_manifest():
    try:
        conn = psycopg2.connect(FEN_CLUSTER_URLS[0])
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT manifest_json FROM cluster_manifest_store WHERE id = 1;")
                row = cur.fetchone()
                if row and row[0]:
                    data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                    return _normalize_manifest(data)
        finally:
            conn.close()
    except Exception as e:
        print(f"Manifest DB load fallback to local file: {e}")

    if os.path.exists(MANIFEST_FILE):
        with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
            return _normalize_manifest(json.load(f))

    return _default_manifest()

def save_manifest(manifest):
    try:
        with open(MANIFEST_FILE, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
    except Exception as e:
        print(f"Local manifest write warning: {e}")

    try:
        conn = psycopg2.connect(FEN_CLUSTER_URLS[0])
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS cluster_manifest_store (
                        id INTEGER PRIMARY KEY,
                        manifest_json JSONB NOT NULL,
                        updated_at TIMESTAMPTZ DEFAULT NOW()
                    );
                    INSERT INTO cluster_manifest_store (id, manifest_json, updated_at)
                    VALUES (1, %s::jsonb, NOW())
                    ON CONFLICT (id) DO UPDATE
                    SET manifest_json = EXCLUDED.manifest_json,
                        updated_at = NOW();
                """, (json.dumps(manifest),))
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        print(f"Manifest DB save error: {e}")

def register_3in1_family_link(manifest: dict, c3_idx: int, game_id: int, used_fen_cluster_indices: List[int]):
    c3_name = f"3in1_cluster_{c3_idx + 1}"
    if c3_name not in manifest["3in1_clusters"]:
        manifest["3in1_clusters"][c3_name] = {
            "game_id_start": game_id,
            "game_id_end": game_id,
            "linked_fen_clusters": []
        }
    info = manifest["3in1_clusters"][c3_name]
    if info.get("game_id_start") is None or game_id < info["game_id_start"]:
        info["game_id_start"] = game_id
    if info.get("game_id_end") is None or game_id > info["game_id_end"]:
        info["game_id_end"] = game_id

    linked = set(info.get("linked_fen_clusters", []))
    for f_idx in used_fen_cluster_indices:
        linked.add(f_idx + 1)
    info["linked_fen_clusters"] = sorted(list(linked))

def find_3in1_cluster_for_game(game_id: int, manifest: dict) -> int:
    str_gid = str(game_id)
    if str_gid in manifest.get("moved_games_registry", {}):
        return manifest["moved_games_registry"][str_gid]

    for c_name, info in manifest.get("3in1_clusters", {}).items():
        start_id = info.get("game_id_start", 0)
        end_id = info.get("game_id_end", 0)
        if start_id <= game_id <= end_id:
            return int(c_name.split("_")[-1]) - 1

    max_c3 = min(manifest.get("active_3in1_cluster_idx", 0), len(SERVER_CLUSTERS) - 1)
    for idx in range(max_c3 + 1):
        conn = psycopg2.connect(SERVER_CLUSTERS[idx]["game_data"])
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM game_data WHERE game_id = %s", (game_id,))
                if cur.fetchone():
                    return idx
        finally:
            conn.close()

    return manifest["active_3in1_cluster_idx"]

def select_writable_3in1_cluster_idx(manifest: dict) -> int:
    max_c3 = min(manifest.get("active_3in1_cluster_idx", 0), len(SERVER_CLUSTERS) - 1)
    for idx in range(max_c3 + 1):
        c_g = psycopg2.connect(SERVER_CLUSTERS[idx]["game_data"])
        c_map = psycopg2.connect(SERVER_CLUSTERS[idx]["fen_map"])
        try:
            if max(get_db_size(c_g), get_db_size(c_map)) < MAX_DB_BYTES:
                return idx
        finally:
            c_g.close()
            c_map.close()

    if (max_c3 + 1) < len(SERVER_CLUSTERS):
        manifest["active_3in1_cluster_idx"] = max_c3 + 1
        return max_c3 + 1
    return max_c3

def upsert_fens_with_family_manager(ordered_fens_from_end: List[str], manifest: dict):
    max_fen_idx = min(manifest.get("active_fen_cluster_idx", 0), len(FEN_CLUSTER_URLS) - 1)
    fen_conns = [psycopg2.connect(url) for url in FEN_CLUSTER_URLS[:max_fen_idx + 1]]

    found_fen_map = {}
    touched_fen_cluster_indices = set()
    remaining_fens = list(dict.fromkeys(ordered_fens_from_end))

    try:
        for idx, f_conn in enumerate(fen_conns):
            if not remaining_fens:
                break
            with f_conn.cursor() as cur:
                cur.execute(
                    "SELECT fen_string, fen_id FROM fen_positions WHERE fen_string = ANY(%s)",
                    (remaining_fens,)
                )
                rows = cur.fetchall()
                if rows:
                    touched_fen_cluster_indices.add(idx)
                for row_fen, row_id in rows:
                    found_fen_map[row_fen] = row_id
            remaining_fens = [f for f in remaining_fens if f not in found_fen_map]

        if remaining_fens:
            target_fen_idx = max_fen_idx
            for idx, f_conn in enumerate(fen_conns):
                if get_db_size(f_conn) < MAX_DB_BYTES:
                    target_fen_idx = idx
                    break
            else:
                if (max_fen_idx + 1) < len(FEN_CLUSTER_URLS):
                    target_fen_idx = max_fen_idx + 1
                    new_conn = psycopg2.connect(FEN_CLUSTER_URLS[target_fen_idx])
                    init_table_for_fen(new_conn)
                    fen_conns.append(new_conn)
                    manifest["active_fen_cluster_idx"] = target_fen_idx

            active_fen_conn = fen_conns[target_fen_idx]
            touched_fen_cluster_indices.add(target_fen_idx)

            current_fen_id = manifest["last_fen_id"] + 1
            new_rows = []
            for f_str in remaining_fens:
                found_fen_map[f_str] = current_fen_id
                new_rows.append((current_fen_id, f_str))
                current_fen_id += 1

            with active_fen_conn.cursor() as cur:
                execute_values(
                    cur,
                    "INSERT INTO fen_positions (fen_id, fen_string) VALUES %s ON CONFLICT (fen_string) DO NOTHING",
                    new_rows
                )
            active_fen_conn.commit()
            manifest["last_fen_id"] = current_fen_id - 1

            f_c_name = f"fen_cluster_{target_fen_idx + 1}"
            if f_c_name not in manifest["fen_clusters"]:
                manifest["fen_clusters"][f_c_name] = {
                    "fen_id_start": new_rows[0][0],
                    "fen_id_end": new_rows[-1][0]
                }
            else:
                manifest["fen_clusters"][f_c_name]["fen_id_end"] = max(
                    manifest["fen_clusters"][f_c_name].get("fen_id_end", 0),
                    new_rows[-1][0]
                )

    finally:
        for fc in fen_conns:
            fc.close()

    return found_fen_map, sorted(list(touched_fen_cluster_indices))

def find_fen_ids_and_owning_cluster(target_fen: str, manifest: dict):
    """
    🌟 1-part FEN (အရုပ်အနေအထားသက်သက်) ဖြင့် အမြန်ဆုံး ရှာဖွေပေးပြီး၊
    အကယ်၍ DB ထဲတွင် ယခင် 4-part FEN အဟောင်းများ ရှိနေပါကလည်း B-Tree Prefix Index သုံး၍ ရှာပေးမည်။
    """
    clean_board_fen = clean_fen_str(target_fen)
    if not clean_board_fen:
        return [], None

    max_fen_idx = min(manifest.get("active_fen_cluster_idx", 0), len(FEN_CLUSTER_URLS) - 1)
    for idx in range(max_fen_idx + 1):
        conn_fen = psycopg2.connect(FEN_CLUSTER_URLS[idx])
        try:
            with conn_fen.cursor() as cur:
                # ၁။ Exact match (idx_fen_string Index ကို တိုက်ရိုက်သုံးသည် - အမြန်ဆုံး)
                cur.execute(
                    "SELECT fen_id FROM fen_positions WHERE fen_string = %s LIMIT 1",
                    (clean_board_fen,)
                )
                row = cur.fetchone()
                if row:
                    return [row[0]], (idx + 1)

                # ၂။ Fallback: ယခင် 4-part FEN ဖြင့် သိမ်းထားမိသော အကွက်များအတွက်
                cur.execute(
                    "SELECT fen_id FROM fen_positions WHERE fen_string LIKE %s LIMIT 10",
                    (clean_board_fen + " %",)
                )
                rows = cur.fetchall()
                if rows:
                    return [r[0] for r in rows], (idx + 1)
        finally:
            conn_fen.close()
    return [], None

def batch_insert_fen_game_map(conn_map, fen_ids: List[int], game_id: int):
    """ပွဲတစ်ပွဲ၏ FEN ID အားလုံးကို တစ်ကွက်ချင်း မပို့ဘဲ Query တစ်ကြောင်းတည်းဖြင့် အမြန်ဆုံး ချိတ်ဆက်မည်"""
    if not fen_ids:
        return
    rows = [(f_id, [game_id]) for f_id in set(fen_ids)]
    with conn_map.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO fen_game_map (fen_id, game_ids) VALUES %s
            ON CONFLICT (fen_id) DO UPDATE
            SET game_ids = CASE
                WHEN fen_game_map.game_ids @> EXCLUDED.game_ids THEN fen_game_map.game_ids
                ELSE fen_game_map.game_ids || EXCLUDED.game_ids
            END;
            """,
            rows
        )
    conn_map.commit()

def process_pgn_on_server(pgn_text: str):
    manifest = load_or_init_manifest()
    current_game_id = manifest["last_game_id"] + 1
    pgn_io = io.StringIO(pgn_text)

    c3_idx = select_writable_3in1_cluster_idx(manifest)
    conn_game = psycopg2.connect(SERVER_CLUSTERS[c3_idx]["game_data"])
    conn_moves = psycopg2.connect(SERVER_CLUSTERS[c3_idx]["raw_moves"])
    conn_map = psycopg2.connect(SERVER_CLUSTERS[c3_idx]["fen_map"])
    init_tables_for_3in1(conn_game, conn_moves, conn_map)

    conn_fen_init = psycopg2.connect(FEN_CLUSTER_URLS[0])
    init_table_for_fen(conn_fen_init)
    conn_fen_init.close()

    try:
        while True:
            game = chess.pgn.read_game(pgn_io)
            if game is None:
                break

            headers = game.headers
            white = headers.get("White", "Unknown")
            black = headers.get("Black", "Unknown")
            w_elo = int(headers["WhiteElo"]) if headers.get("WhiteElo", "").isdigit() else None
            b_elo = int(headers["BlackElo"]) if headers.get("BlackElo", "").isdigit() else None
            result = headers.get("Result", "?-?").strip()
            if not result or result in ["*", "?"]:
                result = "?-?"

            with conn_game.cursor() as cur:
                cur.execute("""
                    INSERT INTO game_data (game_id, white, white_elo, black, black_elo, result, event, site, round, eco, game_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (game_id) DO NOTHING
                """, (
                    current_game_id, white, w_elo, black, b_elo, result,
                    headers.get("Event", ""), headers.get("Site", ""),
                    headers.get("Round", ""), headers.get("ECO", ""), headers.get("Date", "")
                ))
            conn_game.commit()

            exporter = chess.pgn.StringExporter(headers=False, variations=False, comments=False)
            moves_text = game.accept(exporter).strip()
            with conn_moves.cursor() as cur:
                cur.execute(
                    "INSERT INTO raw_moves (game_id, moves_text) VALUES (%s, %s) ON CONFLICT (game_id) DO NOTHING",
                    (current_game_id, moves_text)
                )
            conn_moves.commit()

            fens_from_end = extract_fens_from_end_to_start(game)
            found_fen_map, used_fen_indices = upsert_fens_with_family_manager(fens_from_end, manifest)

            fen_ids = [found_fen_map[f_str] for f_str in fens_from_end if f_str in found_fen_map]
            batch_insert_fen_game_map(conn_map, fen_ids, current_game_id)

            register_3in1_family_link(manifest, c3_idx, current_game_id, used_fen_indices)
            manifest["last_game_id"] = current_game_id
            current_game_id += 1

        save_manifest(manifest)
    finally:
        conn_game.close()
        conn_moves.close()
        conn_map.close()

# 🌟 ရှိပြီးသား ပွဲအဟောင်းများကို FEN Index အသစ် ပြန်ဆောက်ပေးမည့် Background Function
def reindex_existing_games_on_server():
    manifest = load_or_init_manifest()
    max_c3 = min(manifest.get("active_3in1_cluster_idx", 0), len(SERVER_CLUSTERS) - 1)

    conn_fen_init = psycopg2.connect(FEN_CLUSTER_URLS[0])
    init_table_for_fen(conn_fen_init)
    conn_fen_init.close()

    for idx in range(max_c3 + 1):
        conn_moves = psycopg2.connect(SERVER_CLUSTERS[idx]["raw_moves"])
        conn_map = psycopg2.connect(SERVER_CLUSTERS[idx]["fen_map"])
        try:
            with conn_map.cursor() as cur:
                cur.execute("CREATE TABLE IF NOT EXISTS fen_game_map (fen_id INTEGER PRIMARY KEY, game_ids INTEGER[]);")
            conn_map.commit()

            with conn_moves.cursor() as cur:
                cur.execute("SELECT game_id, moves_text FROM raw_moves ORDER BY game_id ASC;")
                all_games = cur.fetchall()

            for game_id, moves_text in all_games:
                if not moves_text:
                    continue
                game_obj = chess.pgn.read_game(io.StringIO(moves_text))
                if not game_obj:
                    continue

                fens_from_end = extract_fens_from_end_to_start(game_obj)
                found_fen_map, used_fen_indices = upsert_fens_with_family_manager(fens_from_end, manifest)
                fen_ids = [found_fen_map[f_str] for f_str in fens_from_end if f_str in found_fen_map]
                batch_insert_fen_game_map(conn_map, fen_ids, game_id)

                register_3in1_family_link(manifest, idx, game_id, used_fen_indices)
                if game_id > manifest["last_game_id"]:
                    manifest["last_game_id"] = game_id

            save_manifest(manifest)
        finally:
            conn_moves.close()
            conn_map.close()

@app.get("/")
def health_check():
    return {"status": "online", "service": "Victory Chess 3+1 Family Cluster API"}

@app.get("/clusters/manifest")
def get_cluster_manifest():
    return load_or_init_manifest()

@app.post("/games/reindex-all")
def trigger_reindex_all(background_tasks: BackgroundTasks):
    """fen_positions Table ဖျက်မိထားပါက ရှိပြီးသားပွဲအားလုံးကို FEN Index ပြန်ဆောက်ရန်"""
    background_tasks.add_task(reindex_existing_games_on_server)
    return {"status": "reindexing_started", "message": "ရှိပြီးသား ပွဲစဉ်အားလုံးကို နောက်ကွယ်တွင် FEN Index ပြန်ဆောက်ပေးနေပါပြီ။"}

@app.post("/upload-pgn")
async def upload_pgn(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    content = await file.read()
    pgn_text = content.decode("utf-8", errors="replace")
    background_tasks.add_task(process_pgn_on_server, pgn_text)
    return {"status": "started", "message": "Server ပေါ်တွင် PGN ကို ခွဲထုတ်သိမ်းဆည်းနေပါပြီ။"}

@app.post("/games/single")
def add_single_game(payload: GamePayload, background_tasks: BackgroundTasks):
    w_elo_str = str(payload.white_elo) if payload.white_elo is not None else ""
    b_elo_str = str(payload.black_elo) if payload.black_elo is not None else ""
    pgn_str = (
        f'[Event "{payload.event}"]\n'
        f'[Site "{payload.site}"]\n'
        f'[Date "{payload.game_date}"]\n'
        f'[Round "{payload.round}"]\n'
        f'[White "{payload.white}"]\n'
        f'[Black "{payload.black}"]\n'
        f'[Result "{payload.result}"]\n'
        f'[WhiteElo "{w_elo_str}"]\n'
        f'[BlackElo "{b_elo_str}"]\n'
        f'[ECO "{payload.eco}"]\n\n'
        f'{payload.moves_text} {payload.result}\n\n'
    )
    background_tasks.add_task(process_pgn_on_server, pgn_str)
    return {"status": "added", "message": "ပွဲစဉ်အသစ်ကို ထည့်သွင်းနေပါပြီ။"}

@app.get("/games/search")
def search_games(
    q: str = "",
    white: str = "",
    white_elo: Optional[int] = None,
    black: str = "",
    black_elo: Optional[int] = None,
    ignore_colours: bool = False,
    tournament: str = "",
    year_start: Optional[int] = None,
    year_end: Optional[int] = None,
    show_win: bool = True,
    show_draw: bool = True,
    show_loss: bool = True,
    use_position: bool = False,
    fen: str = ""
):
    manifest = load_or_init_manifest()
    max_c3 = min(manifest.get("active_3in1_cluster_idx", 0), len(SERVER_CLUSTERS) - 1)
    results = []

    target_fen_ids = []
    owning_fen_cluster_num = None
    clean_board_fen = clean_fen_str(fen)

    # မူလအစ Position မဟုတ်မှသာ FEN Table တွင် ရှာမည်
    if use_position and clean_board_fen and clean_board_fen != INITIAL_BOARD_FEN:
        target_fen_ids, owning_fen_cluster_num = find_fen_ids_and_owning_cluster(clean_board_fen, manifest)
        if not target_fen_ids:
            return []

    for idx in range(max_c3 + 1):
        candidate_game_ids = None
        if target_fen_ids:
            conn_map = psycopg2.connect(SERVER_CLUSTERS[idx]["fen_map"])
            try:
                with conn_map.cursor() as map_cur:
                    map_cur.execute(
                        "SELECT game_ids FROM fen_game_map WHERE fen_id = ANY(%s)",
                        (target_fen_ids,)
                    )
                    m_rows = map_cur.fetchall()
                    if not m_rows:
                        continue
                    merged_ids = set()
                    for r in m_rows:
                        if r[0]:
                            merged_ids.update(r[0])
                    if not merged_ids:
                        continue
                    candidate_game_ids = list(merged_ids)
            finally:
                conn_map.close()

        conn_game = psycopg2.connect(SERVER_CLUSTERS[idx]["game_data"])
        try:
            conditions = []
            params = []

            if candidate_game_ids is not None:
                conditions.append("g.game_id = ANY(%s)")
                params.append(candidate_game_ids)

            if q.strip():
                if q.strip().isdigit():
                    conditions.append("g.game_id = %s")
                    params.append(int(q.strip()))
                else:
                    like_q = f"%{q.strip()}%"
                    conditions.append("(g.white ILIKE %s OR g.black ILIKE %s OR g.event ILIKE %s OR g.eco ILIKE %s OR g.game_date ILIKE %s)")
                    params.extend([like_q, like_q, like_q, like_q, like_q])

            w_clean = white.strip()
            b_clean = black.strip()
            if ignore_colours:
                if w_clean and b_clean:
                    conditions.append("((g.white ILIKE %s AND g.black ILIKE %s) OR (g.white ILIKE %s AND g.black ILIKE %s))")
                    params.extend([f"%{w_clean}%", f"%{b_clean}%", f"%{b_clean}%", f"%{w_clean}%"])
                elif w_clean:
                    conditions.append("(g.white ILIKE %s OR g.black ILIKE %s)")
                    params.extend([f"%{w_clean}%", f"%{w_clean}%"])
                elif b_clean:
                    conditions.append("(g.white ILIKE %s OR g.black ILIKE %s)")
                    params.extend([f"%{b_clean}%", f"%{b_clean}%"])
            else:
                if w_clean:
                    conditions.append("g.white ILIKE %s")
                    params.append(f"%{w_clean}%")
                if b_clean:
                    conditions.append("g.black ILIKE %s")
                    params.append(f"%{b_clean}%")

            if white_elo is not None and white_elo > 0:
                conditions.append("g.white_elo >= %s")
                params.append(white_elo)
            if black_elo is not None and black_elo > 0:
                conditions.append("g.black_elo >= %s")
                params.append(black_elo)

            if tournament.strip():
                conditions.append("g.event ILIKE %s")
                params.append(f"%{tournament.strip()}%")

            if year_start is not None or year_end is not None:
                y_start = year_start if year_start is not None else 0
                y_end = year_end if year_end is not None else 9999
                conditions.append(
                    "CAST(NULLIF(REGEXP_REPLACE(SUBSTRING(g.game_date FROM 1 FOR 4), '[^0-9]', '', 'g'), '') AS INTEGER) BETWEEN %s AND %s"
                )
                params.extend([y_start, y_end])

            allowed_results = []
            if show_win:
                allowed_results.append("1-0")
            if show_draw:
                allowed_results.extend(["1/2-1/2", "1/2"])
            if show_loss:
                allowed_results.append("0-1")

            if len(allowed_results) < 4:
                if not allowed_results:
                    continue
                conditions.append("g.result = ANY(%s)")
                params.append(allowed_results)

            where_sql = " WHERE " + " AND ".join(conditions) if conditions else ""

            # 🌟 N+1 Query လုံးဝမဖြစ်စေရန် LEFT JOIN ဖြင့် တစ်ကြိမ်တည်း အမြန်ဆုံး ဆွဲထုတ်မည်
            query_sql = f"""
                SELECT g.*, COALESCE(m.moves_text, '') AS moves_text
                FROM game_data g
                LEFT JOIN raw_moves m ON g.game_id = m.game_id
                {where_sql}
                ORDER BY g.game_id DESC
                LIMIT 50
            """

            with conn_game.cursor() as cur:
                cur.execute(query_sql, tuple(params))
                columns = [desc[0] for desc in cur.description]
                rows = [dict(zip(columns, r)) for r in cur.fetchall()]

            for r in rows:
                moves_str = r.get("moves_text") or ""
                r["moves_text"] = moves_str
                r["moves"] = moves_str
                r["whiteElo"] = r.get("white_elo")
                r["blackElo"] = r.get("black_elo")
                r["date"] = r.get("game_date")
                results.append(r)

        finally:
            conn_game.close()

    return results[:50]
