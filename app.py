import os
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

# セッション＆管理用
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret-change-me")
ADMIN_PASS = os.environ.get("ADMIN_PASS", None)  # 管理者用共有パスワード

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
    song = db.Column(db.String(500), nullable=False)    # 曲名
    singer = db.Column(db.String(300), nullable=True)   # 歌手名
    user = db.Column(db.String(200), nullable=False)    # ユーザー名
    score = db.Column(db.Float, nullable=False)         # スコア
    date = db.Column(db.DateTime, nullable=False)       # 日付（日時）
    __table_args__ = (UniqueConstraint('song', 'user', 'date', name='_song_user_date_uc'),)

    def to_record(self):
        return {"曲名": self.song, "歌手名": self.singer, "ユーザー": self.user, "スコア": self.score, "日付": self.date}

with app.app_context():
    db.create_all()

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
def fetch_damtomo_ai_scores(username, cookies, max_pages=40):
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
    """'YYYY-MM-DD', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DDTHH:MM' などを受け入れる"""
    if not s:
        return None
    s = s.strip()
    try:
        # HTML datetime-local は 'YYYY-MM-DDTHH:MM'
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

# ---- Public Routes ----
@app.route("/", methods=["GET"])
def home():
    return render_template("home.html")

@app.route("/ranking", methods=["GET"])
def ranking():
    # 検索パラメータ
    song_query = request.args.get("song", "")
    singer_query = request.args.get("singer", "")

    df_all = df_from_db()

    # 統計（省略：ここはあなたの最新版のまま / 以前の機能）
    user_averages = {}
    first_place_counts = {}
    user_unique_song_counts = {}
    total_unique_songs = 0

    if not df_all.empty:
        user_averages = df_all.groupby("ユーザー")["スコア"].mean().round(2).to_dict()
        user_unique_song_counts = df_all.groupby("ユーザー")["曲名"].nunique().to_dict()
        total_unique_songs = int(df_all["曲名"].nunique())

    # フィルタ
    if song_query:
        df_all = df_all[df_all["曲名"].fillna("").str.contains(song_query, case=False, na=False)]
    if singer_query:
        df_all = df_all[df_all["歌手名"].fillna("").str.contains(singer_query, case=False, na=False)]

    # 最高点達成時の行抽出（同点は最初に達成）
    best_rows = pd.DataFrame(columns=["曲名", "ユーザー", "歌手名", "スコア", "日付"])
    if not df_all.empty:
        ordered = df_all.sort_values(["スコア", "日付"], ascending=[False, True])
        best_rows = (
            ordered.groupby(["曲名", "ユーザー"], as_index=False)
                   .first()[["曲名", "ユーザー", "歌手名", "スコア", "日付"]]
        )

    # 曲ごとTop3 → 1位スコア降順で並べ替え
    ranking_list = []
    if not best_rows.empty:
        for song, group in best_rows.groupby("曲名"):
            g_sorted = group.sort_values(["スコア", "日付"], ascending=[False, True])
            top3 = g_sorted.head(3).to_dict(orient="records")
            top_score = float(g_sorted.iloc[0]["スコア"])
            ranking_list.append({"song": song, "top_score": top_score, "records": top3, "singer": g_sorted.iloc[0]["歌手名"]})
            winner = g_sorted.iloc[0]["ユーザー"]
            first_place_counts[winner] = first_place_counts.get(winner, 0) + 1

        ranking_list.sort(key=lambda x: x["top_score"], reverse=True)

    return render_template(
        "ranking.html",
        ranking_list=ranking_list,
        user_averages=user_averages,
        first_place_counts=first_place_counts,
        user_unique_song_counts=user_unique_song_counts,
        total_unique_songs=total_unique_songs,
        song_query=song_query,
        singer_query=singer_query,
    )

@app.route("/update_ranking", methods=["POST"])
def update_ranking():
    song_query = request.form.get("song", "")
    singer_query = request.form.get("singer", "")

    total_inserted = 0
    for user, cookies in USER_COOKIES.items():
        if not cookies.get("scr_cdm"):
            continue
        df_new = fetch_damtomo_ai_scores(user, cookies)
        if not df_new.empty:
            inserted = insert_scores_from_df(df_new)
            total_inserted += inserted
            print(f"[update] {user}: inserted {inserted} rows")

    # 再描画は ranking() と同じロジックを再利用
    return redirect(url_for("ranking", song=song_query, singer=singer_query))

# ---- Admin Routes ----
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

@app.route("/admin/logout")
def admin_logout():
    session.pop("is_admin", None)
    flash("ログアウトしました。", "info")
    return redirect(url_for("admin_login"))

@app.route("/admin", methods=["GET"])
@admin_required
def admin():
    # クエリ（検索）
    q_song = request.args.get("song", "").strip()
    q_user = request.args.get("user", "").strip()
    q_singer = request.args.get("singer", "").strip()

    query = Score.query
    if q_song:
        query = query.filter(Score.song.ilike(f"%{q_song}%"))
    if q_user:
        query = query.filter(Score.user.ilike(f"%{q_user}%"))
    if q_singer:
        query = query.filter(Score.singer.ilike(f"%{q_singer}%"))

    # 新しい順で最大200件
    rows = (query.order_by(Score.date.desc())
                 .limit(200)
                 .all())

    return render_template("admin.html", rows=rows, q_song=q_song, q_user=q_user, q_singer=q_singer)

@app.route("/admin/add", methods=["POST"])
@admin_required
def admin_add():
    song = (request.form.get("song") or "").strip()
    singer = (request.form.get("singer") or "").strip() or None
    user = (request.form.get("user") or "").strip()
    score_str = (request.form.get("score") or "").strip()
    date_str = (request.form.get("date") or "").strip()

    # バリデーション
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

    # 追加（重複は一意制約に委ねる）
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
    
# ---- 起動 ----
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)