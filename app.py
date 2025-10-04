import os
import io
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from flask import Flask, render_template, request, redirect, url_for, session, flash
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import UniqueConstraint
from sqlalchemy.exc import IntegrityError
from datetime import datetime

# ---- Flask + DB setup ----
app = Flask(__name__)

app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret-change-me")
ADMIN_PASS = os.environ.get("ADMIN_PASS", None)

app.config["MAX_CONTENT_LENGTH"] = 5 * 1024 * 1024

DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL:
    app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL.replace("postgres://", "postgresql://", 1)
else:
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///scores.db"

app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)

# ---- DB model ----
class Score(db.Model):
    __tablename__ = "scores"
    id = db.Column(db.Integer, primary_key=True)
    song = db.Column(db.String(500), nullable=False)
    singer = db.Column(db.String(300), nullable=True)
    user = db.Column(db.String(200), nullable=False)
    score = db.Column(db.Float, nullable=False)
    date = db.Column(db.DateTime, nullable=False)
    __table_args__ = (UniqueConstraint('song', 'user', 'date', name='_song_user_date_uc'),)

    def to_record(self):
        return {"曲名": self.song, "歌手名": self.singer, "ユーザー": self.user, "スコア": self.score, "日付": self.date}

with app.app_context():
    db.create_all()

@app.template_filter("fmtdate")
def fmtdate(value, fmt="%Y-%m-%d"):
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        try:
            return value.strftime(fmt)
        except Exception:
            pass
    try:
        dt = datetime.fromisoformat(str(value))
        return dt.strftime(fmt)
    except Exception:
        return str(value)[:10]

# ---- Config / constants ----
AI_SCORE_URL = "https://www.clubdam.com/app/damtomo/scoring/GetScoringAiListXML.do"

USER_COOKIES = {
    "まつりく": {
        "dam-uid": os.environ.get("U1_DAM_UID", ""),
        "scr_cdm": os.environ.get("U1_SCR_CDM", ""),
        "scr_dt":  os.environ.get("U1_SCR_DT",  ""),
        "webmember": "1",
        "wm_ac": os.environ.get("U1_WM_AC", ""),
        "wm_dm": os.environ.get("U1_WM_DM", "")
    },
    "えす": {
        "dam-uid": os.environ.get("U2_DAM_UID", ""),
        "scr_cdm": os.environ.get("U2_SCR_CDM", ""),
        "scr_dt":  os.environ.get("U2_SCR_DT",  ""),
        "webmember": "1",
        "wm_ac": os.environ.get("U2_WM_AC", ""),
        "wm_dm": os.environ.get("U2_WM_DM", "")
    },
    "こんけあ": {
        "dam-uid": os.environ.get("U3_DAM_UID", ""),
        "scr_cdm": os.environ.get("U3_SCR_CDM", ""),
        "scr_dt":  os.environ.get("U3_SCR_DT",  ""),
        "webmember": "1",
        "wm_ac": os.environ.get("U3_WM_AC", ""),
        "wm_dm": os.environ.get("U3_WM_DM", "")
    }
}

# ---- Helpers ----
def fetch_damtomo_ai_scores(username, cookies, max_pages=30):
    all_scores = []
    for page in range(1, max_pages + 1):
        params = {"cdmCardNo": cookies.get("scr_cdm", ""), "pageNo": page, "detailFlg": 0}
        if not params["cdmCardNo"]:
            break
        try:
            res = requests.get(AI_SCORE_URL, cookies=cookies, params=params, timeout=15)
            res.raise_for_status()
            root = ET.fromstring(res.content)
        except Exception as e:
            print(f"[fetch] {username} page {page} request/parse failed: {e}")
            break

        ns = {"ns": "https://www.clubdam.com/app/damtomo/scoring/GetScoringAiListXML"}
        status = root.find(".//ns:status", ns)
        if status is None or status.text != "OK":
            print(f"[fetch] {username} page {page} status not OK; stopping.")
            break

        scorings = root.findall(".//ns:scoring", ns)
        if not scorings:
            break

        for data in scorings:
            song = data.attrib.get("contentsName", "").strip()
            singer = data.attrib.get("artistName", "").strip()
            date_str = data.attrib.get("scoringDateTime", "").strip()
            try:
                raw = data.text
                if raw is None:
                    continue
                score_val = float(raw) / 1000.0
            except (ValueError, TypeError):
                continue
            all_scores.append([song, singer, username, score_val, date_str])

    df = pd.DataFrame(all_scores, columns=["曲名", "歌手名", "ユーザー", "スコア", "日付"])
    if not df.empty:
        df["日付"] = pd.to_datetime(df["日付"], errors="coerce")
    return df

def insert_scores_from_df(df_new):
    if df_new.empty:
        return 0
    df_new = df_new.copy()
    df_new["日付"] = pd.to_datetime(df_new["日付"], errors="coerce")
    inserted = 0
    session_db = db.session
    for _, r in df_new.iterrows():
        if pd.isna(r["日付"]) or pd.isna(r["スコア"]):
            continue
        song = str(r["曲名"])
        singer = str(r["歌手名"]) if not pd.isna(r["歌手名"]) else None
        user = str(r["ユーザー"])
        score_val = float(r["スコア"])
        date_val = r["日付"].to_pydatetime() if hasattr(r["日付"], "to_pydatetime") else r["日付"]

        exists = session_db.query(Score).filter_by(song=song, user=user, date=date_val).first()
        if exists:
            continue

        s = Score(song=song, singer=singer, user=user, score=score_val, date=date_val)
        session_db.add(s)
        try:
            session_db.commit()
            inserted += 1
        except IntegrityError:
            session_db.rollback()
        except Exception as e:
            session_db.rollback()
            print(f"[insert] unexpected error inserting {song}/{user}/{date_val}: {e}")
    return inserted

def df_from_db():
    rows = db.session.query(Score).all()
    data = [{"曲名": r.song, "歌手名": r.singer, "ユーザー": r.user, "スコア": r.score, "日付": r.date} for r in rows]
    if not data:
        return pd.DataFrame(columns=["曲名", "歌手名", "ユーザー", "スコア", "日付"])
    return pd.DataFrame(data)

def parse_datetime_flexible(s: str):
    if not s:
        return None
    s = s.strip()
    try:
        return datetime.fromisoformat(s)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None

def admin_required(fn):
    from functools import wraps
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not ADMIN_PASS:
            return "管理者パスワード（ADMIN_PASS）が設定されていません。", 503
        if session.get("is_admin"):
            return fn(*args, **kwargs)
        return redirect(url_for("admin_login", next=request.path))
    return wrapper

ALLOWED_CSV_EXTS = {"csv"}
def _allowed_csv(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_CSV_EXTS

def _read_csv_flex(file_storage) -> pd.DataFrame:
    raw = file_storage.read()
    for enc in ("utf-8-sig", "cp932"):
        try:
            buf = io.StringIO(raw.decode(enc))
            df = pd.read_csv(buf)
            break
        except Exception:
            df = None
    if df is None:
        raise ValueError("CSVの読み込みに失敗しました（文字コード不明）。UTF-8 / Shift_JIS を試してください。")

    df.columns = [str(c).strip() for c in df.columns]
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip()
    df = df.dropna(how="all")
    return df

def _normalize_csv_columns(df: pd.DataFrame) -> pd.DataFrame:
    candidates = {
        "曲名": {"曲名", "song", "title", "song_name", "songs"},
        "歌手名": {"歌手名", "artist", "singer", "artist_name"},
        "ユーザー": {"ユーザー", "user", "name", "username"},
        "スコア": {"スコア", "score", "点数"},
        "日付": {"日付", "date", "日時", "scored_at", "time", "datetime"},
    }

    lower_map = {c.lower(): c for c in df.columns}
    target_cols = {}
    for std, opts in candidates.items():
        for o in opts:
            if o.lower() in lower_map:
                target_cols[std] = lower_map[o.lower()]
                break

    required = {"曲名", "ユーザー", "スコア", "日付"}
    if not required.issubset(set(target_cols.keys())):
        missing = required - set(target_cols.keys())
        raise ValueError(f"CSVの必須列が不足しています: {', '.join(missing)}")

    out = pd.DataFrame()
    out["曲名"] = df[target_cols["曲名"]]
    out["歌手名"] = df[target_cols["歌手名"]] if "歌手名" in target_cols else None
    out["ユーザー"] = df[target_cols["ユーザー"]]
    out["スコア"] = pd.to_numeric(df[target_cols["スコア"]], errors="coerce")
    out["日付"] = pd.to_datetime(df[target_cols["日付"]], errors="coerce")
    return out

# ---- Routes ----
@app.route("/", methods=["GET"])
def home():
    return render_template("home.html")

@app.route("/ranking", methods=["GET"])
def ranking():
    song_query = request.args.get("song", "")
    singer_query = request.args.get("singer", "")
    filter_user = request.args.get("filter_user", "")
    filter_type = request.args.get("filter_type", "")

    df_all = df_from_db()

    # --- フィルタ処理 ---
    if not df_all.empty and filter_user:
        if filter_type == "others2":
            song_users = df_all.groupby("曲名")["ユーザー"].unique()
            allowed = song_users[song_users.apply(lambda us: filter_user not in us and len(set(us)) == 2)].index
            df_all = df_all[df_all["曲名"].isin(allowed)]
        elif filter_type == "solo":
            song_users = df_all.groupby("曲名")["ユーザー"].unique()
            allowed = song_users[song_users.apply(lambda us: len(set(us)) == 1 and filter_user in us)].index
            df_all = df_all[df_all["曲名"].isin(allowed)]
        elif filter_type == "95":
            allowed = df_all[(df_all["ユーザー"] == filter_user) & (df_all["スコア"] >= 95)]["曲名"].unique()
            df_all = df_all[df_all["曲名"].isin(allowed)]
        elif filter_type == "dere":
            # 指定ユーザーの曲の最高点を計算し、最高点が80未満の曲だけ残す
            user_df = df_all[df_all["ユーザー"] == filter_user]
            max_scores = user_df.groupby("曲名")["スコア"].max()
            dere_songs = max_scores[max_scores < 80].index
            df_all = df_all[df_all["曲名"].isin(dere_songs)]

    # --- 集計（ユーザーカード用） ---
    user_averages = {}
    first_place_counts = {}
    third_place_counts = {}
    user_total_records = {}
    user_95_counts = {}
    user_dere_counts = {}

    if not df_all.empty:
        # 平均スコア
        user_averages = df_all.groupby("ユーザー")["スコア"].mean().round(2).to_dict()
        # 総曲数
        user_total_records = df_all.groupby("ユーザー").size().to_dict()
        # 95点以上曲数
        df_95 = df_all[df_all["スコア"] >= 95]
        for user, group in df_95.groupby("ユーザー"):
            user_95_counts[user] = group["曲名"].nunique()
        # でれんでれん曲数（最高点が80未満の曲のみ）
        for user, group in df_all.groupby("ユーザー"):
            max_scores = group.groupby("曲名")["スコア"].max()
            user_dere_counts[user] = (max_scores < 80).sum()

    # --- 検索フィルタ ---
    filtered = df_all.copy()
    if song_query:
        filtered = filtered[filtered["曲名"].fillna("").str.contains(song_query, case=False, na=False)]
    if singer_query:
        filtered = filtered[filtered["歌手名"].fillna("").str.contains(singer_query, case=False, na=False)]

    # --- 各曲の最高点を集計（曲別ランキング作成） ---
    best_rows = pd.DataFrame(columns=["曲名", "ユーザー", "歌手名", "スコア", "日付"])
    if not filtered.empty:
        ordered = filtered.sort_values(["スコア", "日付"], ascending=[False, True])
        best_rows = ordered.groupby(["曲名", "ユーザー"], as_index=False).first()

    ranking_list = []
    first_place_counts = {}
    third_place_counts = {}
    if not best_rows.empty:
        for song, group in best_rows.groupby("曲名"):
            g_sorted = group.sort_values(["スコア", "日付"], ascending=[False, True])
            top3_df = g_sorted.head(3)
            top3 = top3_df.to_dict(orient="records")
            ranking_list.append({
                "song": song,
                "top_score": float(g_sorted.iloc[0]["スコア"]),
                "records": top3,
                "singer": g_sorted.iloc[0]["歌手名"]
            })
            # 1位/3位カウント
            for idx, rec in enumerate(top3, start=1):
                u = rec["ユーザー"]
                if idx == 1:
                    first_place_counts[u] = first_place_counts.get(u, 0) + 1
                elif idx == 3:
                    third_place_counts[u] = third_place_counts.get(u, 0) + 1

        ranking_list.sort(key=lambda x: x["top_score"], reverse=True)

    return render_template(
        "ranking.html",
        ranking_list=ranking_list,
        result_count=len(ranking_list),
        user_averages=user_averages,
        first_place_counts=first_place_counts,
        third_place_counts=third_place_counts,
        user_total_records=user_total_records,
        user_95_counts=user_95_counts,
        user_dere_counts=user_dere_counts,
        song_query=song_query,
        singer_query=singer_query,
        filter_user=filter_user,
        filter_type=filter_type,
    )
@app.route("/update_ranking", methods=["POST"])
def update_ranking():
    song_query = request.form.get("song", "")
    singer_query = request.form.get("singer", "")
    filter_user = request.form.get("filter_user", "")
    filter_type = request.form.get("filter_type", "")
    total_inserted = 0
    for user, cookies in USER_COOKIES.items():
        if not cookies.get("scr_cdm"):
            continue
        df_new = fetch_damtomo_ai_scores(user, cookies)
        if not df_new.empty:
            inserted = insert_scores_from_df(df_new)
            total_inserted += inserted
            print(f"[update] {user}: inserted {inserted} rows")
    flash(f"ランキング更新完了: {total_inserted} 件追加", "info")
    return redirect(request.referrer or url_for("ranking", song=song_query,
                                                singer=singer_query,
                                                filter_user=filter_user,
                                                filter_type=filter_type))

@app.route("/user/<username>", methods=["GET"])
def user_history(username):
    sort = request.args.get("sort", "recent")
    song_query = request.args.get("song", "").strip()
    singer_query = request.args.get("singer", "").strip()
    per = int(request.args.get("per", 50) or 50)
    page = int(request.args.get("page", 1) or 1)

    df = df_from_db()
    if df.empty:
        records, total = [], 0
    else:
        df = df[df["ユーザー"] == username]
        if song_query:
            df = df[df["曲名"].fillna("").str.contains(song_query, case=False, na=False)]
        if singer_query:
            df = df[df["歌手名"].fillna("").str.contains(singer_query, case=False, na=False)]
        if sort == "recent":
            df = df.sort_values("日付", ascending=False)
        elif sort == "oldest":
            df = df.sort_values("日付", ascending=True)
        elif sort == "score_low":
            df = df.sort_values(["スコア", "日付"], ascending=[True, True])
        else:
            df = df.sort_values(["スコア", "日付"], ascending=[False, True])

        total = len(df)
        start = (page - 1) * per
        end = start + per
        page_df = df.iloc[start:end]
        records = page_df.to_dict(orient="records")

    total_pages = max((total + per - 1) // per, 1)
    return render_template("user_history.html",
                           username=username, records=records, total=total,
                           sort=sort, song_query=song_query, singer_query=singer_query,
                           per=per, page=page, total_pages=total_pages)

@app.route("/user/<username>/thirds", methods=["GET"])
def user_third_rank(username):
    df_all = df_from_db()
    ranking_cards = []
    if not df_all.empty:
        ordered = df_all.sort_values(["スコア", "日付"], ascending=[False, True])
        best_rows = (
            ordered.groupby(["曲名", "ユーザー"], as_index=False)
                   .first()[["曲名", "ユーザー", "歌手名", "スコア", "日付"]]
        )
        for song, group in best_rows.groupby("曲名"):
            g_sorted = group.sort_values(["スコア", "日付"], ascending=[False, True])
            top3_df = g_sorted.head(3)
            if len(top3_df) >= 3 and top3_df.iloc[2]["ユーザー"] == username:
                ranking_cards.append({
                    "song": song,
                    "singer": g_sorted.iloc[0]["歌手名"],
                    "records": top3_df.to_dict(orient="records")
                })
    ranking_cards.sort(key=lambda x: x["records"][0]["スコア"] if x["records"] else 0, reverse=True)
    return render_template("user_third.html", username=username, ranking_cards=ranking_cards)

@app.route("/history/all", methods=["GET"])
def all_history():
    sort = request.args.get("sort", "recent")
    song_query = request.args.get("song", "").strip()
    singer_query = request.args.get("singer", "").strip()
    per = int(request.args.get("per", 50) or 50)
    page = int(request.args.get("page", 1) or 1)

    df = df_from_db()
    if df.empty:
        records, total = [], 0
    else:
        if song_query:
            df = df[df["曲名"].fillna("").str.contains(song_query, case=False, na=False)]
        if singer_query:
            df = df[df["歌手名"].fillna("").str.contains(singer_query, case=False, na=False)]
        if sort == "recent":
            df = df.sort_values("日付", ascending=False)
        elif sort == "oldest":
            df = df.sort_values("日付", ascending=True)
        elif sort == "score_low":
            df = df.sort_values(["スコア", "日付"], ascending=[True, True])
        else:
            df = df.sort_values(["スコア", "日付"], ascending=[False, True])

        total = len(df)
        start = (page - 1) * per
        end = start + per
        page_df = df.iloc[start:end]
        records = page_df.to_dict(orient="records")

    total_pages = max((total + per - 1) // per, 1)
    return render_template("all_history.html",
                           records=records, total=total, sort=sort,
                           song_query=song_query, singer_query=singer_query,
                           per=per, page=page, total_pages=total_pages)

@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        pw = request.form.get("password", "")
        if not ADMIN_PASS:
            flash("環境変数 ADMIN_PASS が未設定です。", "error")
        elif pw == ADMIN_PASS:
            session["is_admin"] = True
            next_url = request.args.get("next") or url_for("admin")
            return redirect(next_url)
        else:
            flash("パスワードが違います。", "error")
    return render_template("admin_login.html")

@app.route("/admin", methods=["GET"])
@admin_required
def admin():
    return render_template("admin.html")

@app.route("/admin/add", methods=["POST"])
@admin_required
def admin_add():
    song = (request.form.get("song") or "").strip()
    singer = (request.form.get("singer") or "").strip() or None
    user = (request.form.get("user") or "").strip()
    score_str = (request.form.get("score") or "").strip()
    date_str = (request.form.get("date") or "").strip()

    errors = []
    if not song: errors.append("曲名は必須です。")
    if not user: errors.append("ユーザーは必須です。")
    try:
        score_val = float(score_str)
    except Exception:
        errors.append("スコアは数値で入力してください。")
        score_val = None
    date_val = parse_datetime_flexible(date_str)
    if date_val is None:
        errors.append("日付の形式が不正です。YYYY-MM-DD または YYYY-MM-DDTHH:MM で入力してください。")

    if errors:
        for e in errors:
            flash(e, "error")
        return redirect(url_for("admin"))

    s = Score(song=song, singer=singer, user=user, score=score_val, date=date_val)
    db.session.add(s)
    try:
        db.session.commit()
        flash("1件追加しました。", "info")
    except IntegrityError:
        db.session.rollback()
        flash("同じ（曲名・ユーザー・日時）のデータが既に存在します。", "error")
    except Exception as e:
        db.session.rollback()
        flash(f"予期せぬエラー: {e}", "error")
    return redirect(url_for("admin"))

@app.route("/admin/delete", methods=["POST"])
@admin_required
def admin_delete():
    song = (request.form.get("del_song") or "").strip()
    user = (request.form.get("del_user") or "").strip()
    date_str = (request.form.get("del_date") or "").strip()

    if not song or not user or not date_str:
        flash("曲名・ユーザー・日時は必須です。", "error")
        return redirect(url_for("admin"))

    date_val = parse_datetime_flexible(date_str)
    if date_val is None:
        flash("日時の形式が不正です。YYYY-MM-DD または YYYY-MM-DDTHH:MM で入力してください。", "error")
        return redirect(url_for("admin"))

    row = Score.query.filter_by(song=song, user=user, date=date_val).first()
    if not row:
        flash("該当レコードが見つかりません（曲名・ユーザー・日時の完全一致で検索）。", "error")
        return redirect(url_for("admin"))

    try:
        db.session.delete(row)
        db.session.commit()
        flash("1件削除しました。", "info")
    except Exception as e:
        db.session.rollback()
        flash(f"削除でエラーが発生しました: {e}", "error")
    return redirect(url_for("admin"))

@app.route("/admin/delete_namaoto", methods=["POST"])
@admin_required
def delete_namaoto():
    try:
        deleted_count = Score.query.filter(Score.song.contains("生音")).delete(synchronize_session=False)
        db.session.commit()
        flash(f"『生音』を含む曲データを {deleted_count} 件削除しました。", "info")
    except Exception as e:
        db.session.rollback()
        flash(f"削除中にエラーが発生しました: {e}", "error")
    return redirect(url_for("admin"))

@app.route("/admin/import", methods=["POST"])
@admin_required
def admin_import():
    file = request.files.get("csvfile")
    if not file or file.filename == "":
        flash("CSVファイルを選択してください。", "error")
        return redirect(url_for("admin"))
    if not _allowed_csv(file.filename):
        flash("CSV以外の拡張子は受け付けていません。", "error")
        return redirect(url_for("admin"))

    try:
        df_raw = _read_csv_flex(file)
        df_norm = _normalize_csv_columns(df_raw)
        before = len(df_norm)
        df_norm = df_norm.dropna(subset=["曲名", "ユーザー", "スコア", "日付"])
        dropped = before - len(df_norm)
        inserted = insert_scores_from_df(df_norm)
        msg = f"CSV取り込み完了: 受領 {before} 行 / 無効 {dropped} 行 / 追加 {inserted} 行"
        flash(msg, "info")
    except ValueError as ve:
        flash(str(ve), "error")
    except Exception as e:
        flash(f"CSV取り込み中にエラー: {e}", "error")

    return redirect(url_for("admin"))

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

