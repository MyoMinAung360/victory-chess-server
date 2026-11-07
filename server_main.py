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
INITIAL_BOARD_FEN_4 = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -"

# ---------------------------------------------------------
# Supabase PostgreSQL Connection URLs (Project ၂ လုံးစာ ဖွဲ့စည်းပုံ)
# ---------------------------------------------------------

# ၁။ FEN Cluster Group အတွက် (Project 1)
FEN_CLUSTER_URLS = [
    "postgres:2006ChessMandalay@db.lzeqykackqajwfaqpzgq.supabase.co:5432/postgres",
]

# ၂. Service Clusters Group အတွက် (Project 2)
SERVER_CLUSTERS = [
    {
        "game_data": "postgres:2006ChessMandalay@db.czekgqvmpugtydfxvlcu.supabase.co:5432/postgres",
        "raw_moves": "postgres:2006ChessMandalay@db.czekgqvmpugtydfxvlcu.supabase.co:5432/postgres",
        "fen_map":   "postgres:2006ChessMandalay@db.lzeqykackqajwfaqpzgq.supabase.co:5432/postgres",
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

def get_clean_fen(board):
    return " ".join(board.fen().split(" ")[:4])

def clean_fen_str(fen_str: str) -> str:
    return " ".join(fen_str.strip().split()[:4])

def extract_fens_from_end_to_start(game) -> List[str]:
    """
    ပွဲစဉ်တစ်ပွဲ၏ ရွှေ့ကွက်များကို အစမှအဆုံး တွက်ချက်ပြီးနောက်
    FEN Central Base သို့ ထည့်သွင်းရာတွင် ကစားပွဲ၏ နောက်ဆုံးအကွက် (End Position) မှ
    ပထမဆုံးအကွက်သို့ ပြောင်းပြန် (Reverse) စီပေးသည့် Function
    """
    board = game.board()
    fens_in_order = []
    for move in game.mainline_moves():
        board.push(move)
        c_fen = get_clean_fen(board)
        if c_fen != INITIAL_BOARD_FEN_4:
            fens_in_order.append(c_fen)

    # နောက်ဆုံးအကွက်မှ စတင်၍ ရှေ့သို့ ပြောင်းပြန်စီပြီး Duplicate ဖယ်ရှားမည် (အစီအစဉ် မပျက်စေရပါ)
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

# =========================================================================
# 🌟 SERVERLESS-SAFE 3 + 1 FAMILY CLUSTER MANAGER
# (Free Serverless အိပ်သွားလျှင်လည်း ဒေတာမပျောက်စေရန် Supabase DB ထဲ၌ သိမ်းဆည်းပေးသည်)
# =========================================================================
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
    # ၁။ ပထမဦးစွာ Primary Supabase DB (FEN_CLUSTER_URLS[0]) ထဲရှိ cluster_manifest_store table မှ ဖတ်မည်
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
                """)
                conn.commit()
                cur.execute("SELECT manifest_json FROM cluster_manifest_store WHERE id = 1;")
                row = cur.fetchone()
                if row and row[0]:
                    data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                    normalized = _normalize_manifest(data)
                    with open(MANIFEST_FILE, "w", encoding="utf-8") as f:
                        json.dump(normalized, f, indent=2)
                    return normalized
        finally:
            conn.close()
    except Exception as e:
        print(f"Manifest DB load fallback to local file: {e}")

    # ၂။ အကယ်၍ DB ထဲတွင် မရှိသေးပါက Local file မှ ဖတ်မည်
    if os.path.exists(MANIFEST_FILE):
        with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
            return _normalize_manifest(json.load(f))

    return _default_manifest()

def save_manifest(manifest):
    # ၁။ Local cache ဖိုင်တွင် သိမ်းမည်
    try:
        with open(MANIFEST_FILE, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
    except Exception as e:
        print(f"Local manifest write warning: {e}")

    # ၂။ Primary Supabase DB ထဲတွင် အမြဲတမ်း မပျောက်ပျက်အောင် သိမ်းဆည်းမည်
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
    """3-in-1 Cluster နှင့် FEN Cluster များအကြား မိသားစုချိတ်ဆက်မှု (3+1 Family) ကို မှတ်တမ်းတင်ခြင်း"""
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
    """Game ID တစ်ခုသည် မည်သည့် 3in1 Cluster တွင် ရှိနေသည်ကို အတိအကျ ရှာဖွေပေးခြင်း"""
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
    """
    Senior Cluster များတွင် ဖျက်ထား၍ နေရာလွတ် (450MB အောက်) ပြန်ပေါ်လာပါက
    ရှေ့ Senior Cluster သို့ အလိုအလျောက် ပြန်ထည့်ပေးပြီး၊ ပြည့်နေမှ Junior Cluster သစ်သို့ ပို့ပေးမည်။
    """
    max_c3 = min(manifest.get("active_3in1_cluster_idx", 0), len(SERVER_CLUSTERS) - 1)
    for idx in range(max_c3 + 1):
        c_g = psycopg2.connect(SERVER_CLUSTERS[idx]["game_data"])
        c_m = psycopg2.connect(SERVER_CLUSTERS[idx]["raw_moves"])
        c_map = psycopg2.connect(SERVER_CLUSTERS[idx]["fen_map"])
        try:
            if max(get_db_size(c_g), get_db_size(c_m), get_db_size(c_map)) < MAX_DB_BYTES:
                return idx
        finally:
            c_g.close()
            c_m.close()
            c_map.close()

    if (max_c3 + 1) < len(SERVER_CLUSTERS):
        manifest["active_3in1_cluster_idx"] = max_c3 + 1
        return max_c3 + 1
    return max_c3

def upsert_fens_with_family_manager(ordered_fens_from_end: List[str], manifest: dict):
    """
    🌟 ကစားပွဲ၏ နောက်ဆုံးအကွက်မှ စတင်ပေးပို့လာသော FEN များကို စစ်ဆေးပြီး
    ရှိပြီးသား FEN ဖြစ်က ထပ်မထည့်ဘဲ fen_id ကိုသာ ပြန်ယူကာ (လုံးဝ ရပ်မပစ်ပါ)၊
    မရှိသေးသော FEN များကို နောက်ဆုံးအကွက်မှ စတင်၍ FEN Central Base သို့ ထည့်သွင်းမည်။
    """
    max_fen_idx = min(manifest.get("active_fen_cluster_idx", 0), len(FEN_CLUSTER_URLS) - 1)
    fen_conns = [psycopg2.connect(url) for url in FEN_CLUSTER_URLS[:max_fen_idx + 1]]
    for fc in fen_conns:
        init_table_for_fen(fc)

    found_fen_map = {}
    touched_fen_cluster_indices = set()
    # နောက်ဆုံးအကွက်မှ ပထမဆုံးအကွက်သို့ အစီအစဉ်မပျက် ထိန်းသိမ်းထားမည်
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
            # ရှိပြီးသား FEN များကို အသစ်ထပ်မထည့်ရန် ချန်လှပ်ထားခဲ့မည်
            # (သို့သော် found_fen_map ထဲတွင် ပါဝင်ပြီးဖြစ်၍ fen_game_map ၌ game_id ချိတ်ဆက်မှု လုံးဝမပျက်ပါ)
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
            # remaining_fens သည် နောက်ဆုံးအကွက်မှ စတင်ထားသဖြင့် နောက်ဆုံးအကွက်မှစ၍ fen_id အသစ် ဝင်မည်
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

def find_fen_id_and_owning_cluster(target_fen: str, manifest: dict):
    clean_target = clean_fen_str(target_fen)
    max_fen_idx = min(manifest.get("active_fen_cluster_idx", 0), len(FEN_CLUSTER_URLS) - 1)
    for idx in range(max_fen_idx + 1):
        conn_fen = psycopg2.connect(FEN_CLUSTER_URLS[idx])
        try:
            with conn_fen.cursor() as cur:
                cur.execute("SELECT fen_id FROM fen_positions WHERE fen_string = %s LIMIT 1", (clean_target,))
                row = cur.fetchone()
                if row:
                    return row[0], (idx + 1)
        finally:
            conn_fen.close()
    return None, None

# ---------------------------------------------------------
# PGN ကို Background တွင် စနစ်တကျ ခွဲထုတ်သိမ်းဆည်းခြင်း
# ---------------------------------------------------------
def process_pgn_on_server(pgn_text: str):
    manifest = load_or_init_manifest()
    current_game_id = manifest["last_game_id"] + 1
    pgn_io = io.StringIO(pgn_text)

    while True:
        game = chess.pgn.read_game(pgn_io)
        if game is None:
            break

        c3_idx = select_writable_3in1_cluster_idx(manifest)

        conn_game = psycopg2.connect(SERVER_CLUSTERS[c3_idx]["game_data"])
        conn_moves = psycopg2.connect(SERVER_CLUSTERS[c3_idx]["raw_moves"])
        conn_map = psycopg2.connect(SERVER_CLUSTERS[c3_idx]["fen_map"])
        init_tables_for_3in1(conn_game, conn_moves, conn_map)

        try:
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
                """, (
                    current_game_id, white, w_elo, black, b_elo, result,
                    headers.get("Event", ""), headers.get("Site", ""),
                    headers.get("Round", ""), headers.get("ECO", ""), headers.get("Date", "")
                ))
            conn_game.commit()

            exporter = chess.pgn.StringExporter(headers=False, variations=False, comments=False)
            moves_text = game.accept(exporter).strip()
            with conn_moves.cursor() as cur:
                cur.execute("INSERT INTO raw_moves (game_id, moves_text) VALUES (%s, %s)", (current_game_id, moves_text))
            conn_moves.commit()

            # 🌟 ကစားပွဲ၏ နောက်ဆုံးအကွက်မှ ပထမဆုံးအကွက်သို့ ပြောင်းပြန်စီ၍ FEN Central Base သို့ ပို့မည်
            fens_from_end = extract_fens_from_end_to_start(game)
            found_fen_map, used_fen_indices = upsert_fens_with_family_manager(fens_from_end, manifest)

            with conn_map.cursor() as cur:
                for f_str in fens_from_end:
                    f_id = found_fen_map.get(f_str)
                    if f_id is not None:
                        cur.execute("""
                            INSERT INTO fen_game_map (fen_id, game_ids) VALUES (%s, %s)
                            ON CONFLICT (fen_id) DO UPDATE
                            SET game_ids = array(SELECT DISTINCT unnest(fen_game_map.game_ids || EXCLUDED.game_ids));
                        """, (f_id, [current_game_id]))
            conn_map.commit()

            register_3in1_family_link(manifest, c3_idx, current_game_id, used_fen_indices)
            manifest["last_game_id"] = current_game_id
            save_manifest(manifest)

            current_game_id += 1
        finally:
            conn_game.close()
            conn_moves.close()
            conn_map.close()

# ---------------------------------------------------------
# API Endpoints: Health, Bulk Upload, Single Add, Dual Search, Edit & Delete
# ---------------------------------------------------------
@app.get("/")
def health_check():
    return {"status": "online", "service": "Victory Chess 3+1 Family Cluster API"}

@app.get("/clusters/manifest")
def get_cluster_manifest():
    return load_or_init_manifest()

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

    target_fen_id = None
    owning_fen_cluster_num = None
    if use_position and fen.strip():
        target_fen_id, owning_fen_cluster_num = find_fen_id_and_owning_cluster(fen, manifest)
        if target_fen_id is None:
            return []

    for idx in range(max_c3 + 1):
        c3_name = f"3in1_cluster_{idx + 1}"
        if owning_fen_cluster_num is not None and c3_name in manifest.get("3in1_clusters", {}):
            linked_fens = manifest["3in1_clusters"][c3_name].get("linked_fen_clusters", [])
            if linked_fens and owning_fen_cluster_num not in linked_fens:
                continue

        conn_game = psycopg2.connect(SERVER_CLUSTERS[idx]["game_data"])
        conn_moves = psycopg2.connect(SERVER_CLUSTERS[idx]["raw_moves"])
        conn_map = psycopg2.connect(SERVER_CLUSTERS[idx]["fen_map"]) if target_fen_id is not None else None

        try:
            candidate_game_ids = None
            if target_fen_id is not None and conn_map is not None:
                with conn_map.cursor() as map_cur:
                    map_cur.execute("SELECT game_ids FROM fen_game_map WHERE fen_id = %s", (target_fen_id,))
                    m_row = map_cur.fetchone()
                    if not m_row or not m_row[0]:
                        continue
                    candidate_game_ids = m_row[0]

            conditions = []
            params = []

            if candidate_game_ids is not None:
                conditions.append("game_id = ANY(%s)")
                params.append(candidate_game_ids)

            if q.strip():
                if q.strip().isdigit():
                    conditions.append("game_id = %s")
                    params.append(int(q.strip()))
                else:
                    like_q = f"%{q.strip()}%"
                    conditions.append("(white ILIKE %s OR black ILIKE %s OR event ILIKE %s OR eco ILIKE %s OR game_date ILIKE %s)")
                    params.extend([like_q, like_q, like_q, like_q, like_q])

            w_clean = white.strip()
            b_clean = black.strip()
            if ignore_colours:
                if w_clean and b_clean:
                    conditions.append("((white ILIKE %s AND black ILIKE %s) OR (white ILIKE %s AND black ILIKE %s))")
                    params.extend([f"%{w_clean}%", f"%{b_clean}%", f"%{b_clean}%", f"%{w_clean}%"])
                elif w_clean:
                    conditions.append("(white ILIKE %s OR black ILIKE %s)")
                    params.extend([f"%{w_clean}%", f"%{w_clean}%"])
                elif b_clean:
                    conditions.append("(white ILIKE %s OR black ILIKE %s)")
                    params.extend([f"%{b_clean}%", f"%{b_clean}%"])
            else:
                if w_clean:
                    conditions.append("white ILIKE %s")
                    params.append(f"%{w_clean}%")
                if b_clean:
                    conditions.append("black ILIKE %s")
                    params.append(f"%{b_clean}%")

            if white_elo is not None and white_elo > 0:
                conditions.append("white_elo >= %s")
                params.append(white_elo)
            if black_elo is not None and black_elo > 0:
                conditions.append("black_elo >= %s")
                params.append(black_elo)

            if tournament.strip():
                conditions.append("event ILIKE %s")
                params.append(f"%{tournament.strip()}%")

            if year_start is not None or year_end is not None:
                y_start = year_start if year_start is not None else 0
                y_end = year_end if year_end is not None else 9999
                conditions.append(
                    "CAST(NULLIF(REGEXP_REPLACE(SUBSTRING(game_date FROM 1 FOR 4), '[^0-9]', '', 'g'), '') AS INTEGER) BETWEEN %s AND %s"
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
                conditions.append("result = ANY(%s)")
                params.append(allowed_results)

            where_sql = " WHERE " + " AND ".join(conditions) if conditions else ""
            query_sql = f"SELECT * FROM game_data {where_sql} ORDER BY game_id DESC LIMIT 50"

            with conn_game.cursor() as cur:
                cur.execute(query_sql, tuple(params))
                columns = [desc[0] for desc in cur.description]
                rows = [dict(zip(columns, r)) for r in cur.fetchall()]

            with conn_moves.cursor() as m_cur:
                for r in rows:
                    m_cur.execute("SELECT moves_text FROM raw_moves WHERE game_id = %s", (r["game_id"],))
                    m_row = m_cur.fetchone()
                    moves_str = m_row[0] if m_row else ""

                    r["moves_text"] = moves_str
                    r["moves"] = moves_str
                    r["whiteElo"] = r.get("white_elo")
                    r["blackElo"] = r.get("black_elo")
                    r["date"] = r.get("game_date")
                    results.append(r)

        finally:
            conn_game.close()
            conn_moves.close()
            if conn_map:
                conn_map.close()

    return results[:50]

@app.put("/games/{game_id}")
def update_game(game_id: int, payload: GamePayload):
    manifest = load_or_init_manifest()
    old_c3_idx = find_3in1_cluster_for_game(game_id, manifest)

    conn_game = psycopg2.connect(SERVER_CLUSTERS[old_c3_idx]["game_data"])
    conn_moves = psycopg2.connect(SERVER_CLUSTERS[old_c3_idx]["raw_moves"])
    conn_map = psycopg2.connect(SERVER_CLUSTERS[old_c3_idx]["fen_map"])

    vacuum_if_needed(conn_game)
    vacuum_if_needed(conn_moves)
    vacuum_if_needed(conn_map)

    with conn_moves.cursor() as cur:
        cur.execute("SELECT moves_text FROM raw_moves WHERE game_id=%s", (game_id,))
        old_row = cur.fetchone()
        old_moves = old_row[0] if old_row else ""

    moves_changed = old_moves.strip() != payload.moves_text.strip()
    cluster_full = max(get_db_size(conn_game), get_db_size(conn_moves), get_db_size(conn_map)) >= MAX_DB_BYTES
    target_c3_idx = old_c3_idx

    if moves_changed and cluster_full:
        new_c3_idx = select_writable_3in1_cluster_idx(manifest)
        if new_c3_idx != old_c3_idx:
            target_c3_idx = new_c3_idx

    if target_c3_idx != old_c3_idx:
        with conn_game.cursor() as cur:
            cur.execute("DELETE FROM game_data WHERE game_id=%s", (game_id,))
        conn_game.commit()
        with conn_moves.cursor() as cur:
            cur.execute("DELETE FROM raw_moves WHERE game_id=%s", (game_id,))
        conn_moves.commit()
        with conn_map.cursor() as cur:
            cur.execute("""
                UPDATE fen_game_map SET game_ids = array_remove(game_ids, %s) WHERE %s = ANY(game_ids);
                DELETE FROM fen_game_map WHERE cardinality(game_ids) = 0;
            """, (game_id, game_id))
        conn_map.commit()
        conn_game.close()
        conn_moves.close()
        conn_map.close()

        conn_game = psycopg2.connect(SERVER_CLUSTERS[target_c3_idx]["game_data"])
        conn_moves = psycopg2.connect(SERVER_CLUSTERS[target_c3_idx]["raw_moves"])
        conn_map = psycopg2.connect(SERVER_CLUSTERS[target_c3_idx]["fen_map"])
        init_tables_for_3in1(conn_game, conn_moves, conn_map)

        with conn_game.cursor() as cur:
            cur.execute("""
                INSERT INTO game_data (game_id, white, white_elo, black, black_elo, result, event, site, round, eco, game_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                game_id, payload.white, payload.white_elo, payload.black, payload.black_elo,
                payload.result, payload.event, payload.site, payload.round, payload.eco, payload.game_date
            ))
        conn_game.commit()

        with conn_moves.cursor() as cur:
            cur.execute("INSERT INTO raw_moves (game_id, moves_text) VALUES (%s, %s)", (game_id, payload.moves_text))
        conn_moves.commit()

        manifest["moved_games_registry"][str(game_id)] = target_c3_idx
    else:
        with conn_game.cursor() as cur:
            cur.execute("""
                UPDATE game_data
                SET white=%s, white_elo=%s, black=%s, black_elo=%s,
                    result=%s, event=%s, site=%s, round=%s, eco=%s, game_date=%s
                WHERE game_id=%s
            """, (
                payload.white, payload.white_elo, payload.black, payload.black_elo,
                payload.result, payload.event, payload.site, payload.round,
                payload.eco, payload.game_date, game_id
            ))
        conn_game.commit()

        if moves_changed:
            with conn_moves.cursor() as cur:
                cur.execute("UPDATE raw_moves SET moves_text=%s WHERE game_id=%s", (payload.moves_text, game_id))
            conn_moves.commit()

            with conn_map.cursor() as m_cur:
                m_cur.execute("""
                    UPDATE fen_game_map SET game_ids = array_remove(game_ids, %s) WHERE %s = ANY(game_ids);
                    DELETE FROM fen_game_map WHERE cardinality(game_ids) = 0;
                """, (game_id, game_id))
            conn_map.commit()

    if moves_changed or (target_c3_idx != old_c3_idx):
        game_obj = chess.pgn.read_game(io.StringIO(payload.moves_text))
        if game_obj:
            # 🌟 ကစားပွဲ၏ နောက်ဆုံးအကွက်မှ ပထမဆုံးအကွက်သို့ ပြောင်းပြန်စီ၍ FEN Central Base သို့ ပို့မည်
            fens_from_end = extract_fens_from_end_to_start(game_obj)
            found_fen_map, used_fen_indices = upsert_fens_with_family_manager(fens_from_end, manifest)

            with conn_map.cursor() as m_cur:
                for f_str in fens_from_end:
                    f_id = found_fen_map.get(f_str)
                    if f_id is not None:
                        m_cur.execute("""
                            INSERT INTO fen_game_map (fen_id, game_ids) VALUES (%s, %s)
                            ON CONFLICT (fen_id) DO UPDATE
                            SET game_ids = array(SELECT DISTINCT unnest(fen_game_map.game_ids || EXCLUDED.game_ids));
                        """, (f_id, [game_id]))
            conn_map.commit()

            register_3in1_family_link(manifest, target_c3_idx, game_id, used_fen_indices)
            save_manifest(manifest)

    conn_game.close()
    conn_moves.close()
    conn_map.close()
    return {"status": "updated", "game_id": game_id, "cluster": target_c3_idx + 1}

@app.delete("/games/{game_id}")
def delete_game(game_id: int):
    manifest = load_or_init_manifest()
    c3_idx = find_3in1_cluster_for_game(game_id, manifest)

    conn_game = psycopg2.connect(SERVER_CLUSTERS[c3_idx]["game_data"])
    conn_moves = psycopg2.connect(SERVER_CLUSTERS[c3_idx]["raw_moves"])
    conn_map = psycopg2.connect(SERVER_CLUSTERS[c3_idx]["fen_map"])

    with conn_game.cursor() as cur:
        cur.execute("DELETE FROM game_data WHERE game_id = %s", (game_id,))
    conn_game.commit()

    with conn_moves.cursor() as cur:
        cur.execute("DELETE FROM raw_moves WHERE game_id = %s", (game_id,))
    conn_moves.commit()

    with conn_map.cursor() as cur:
        cur.execute("""
            UPDATE fen_game_map
            SET game_ids = array_remove(game_ids, %s)
            WHERE %s = ANY(game_ids);
            DELETE FROM fen_game_map WHERE cardinality(game_ids) = 0;
        """, (game_id, game_id))
    conn_map.commit()

    vacuum_if_needed(conn_game)
    vacuum_if_needed(conn_moves)
    vacuum_if_needed(conn_map)

    if str(game_id) in manifest.get("moved_games_registry", {}):
        del manifest["moved_games_registry"][str(game_id)]
        save_manifest(manifest)

    conn_game.close()
    conn_moves.close()
    conn_map.close()
    return {"status": "deleted", "game_id": game_id}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 7860))
    uvicorn.run("server_main:app", host="0.0.0.0", port=port, reload=False)
