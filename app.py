import os
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from flask import Flask, render_template, request, flash
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import UniqueConstraint
from sqlalchemy.exc import IntegrityError
import locale

# ---- Flask + DB setup ----
app = Flask(__name__)

# セッションを使わない設計だが、将来のため SECRET_KEY は一応保持（未設定でも動作）
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret-change-me")

DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL:
    app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL.replace("postgres://", "postgresql://", 1)
else:
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///scores.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)

class Score(db.Model):
    __tablename__ = "scores"
    id = db.Column(db.Integer, primary_key=True)
    song = db.Column(db.String(500), nullable=False)
    singer = db.Column(db.String(300), nullable=True)
    user = db.Column(db.String(200), nullable=False)
    score = db.Column(db.Float, nullable=False)
    date = db.Column(db.DateTime, nullable=False)
    __table_args__ = (UniqueConstraint('song', 'user', 'date', name='_song_user_date_uc'),)

with app.app_context():
    db.create_all()

AI_SCORE_URL = "https://www.clubdam.com/app/damtomo/scoring/GetScoringAiListXML.do"

# ---- Cookie は環境変数からのみ取得（直書き禁止）----
# 例：U1_SCR_CDM, U1_DAM_UID, U1_SCR_DT, U1_WM_AC, U1_WM_DM
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
def ja_sort_key(s: str):
    if s is None:
        return ""
    try:
        locale.setlocale(locale.LC_COLLATE, 'ja_JP.UTF-8')
        return locale.strxfrm(s)
    except Exception:
        return (s or "").casefold()

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
            print(f"[fetch] {username} page {page} failed: {e}")
            break
        ns = {"ns": "https://www.clubdam.com/app/damtomo/scoring/GetScoringAiListXML"}
        status = root.find(".//ns:status", ns)
        if status is None or status.text != "OK":
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
            except Exception:
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
    return pd.DataFrame(data) if data else pd.DataFrame(columns=["曲名", "歌手名", "ユーザー", "スコア", "日付"])

# ---- Routes（完全公開） ----
@app.route("/")
def home():
    return render_template("home.html")

@app.route("/ranking")
def ranking():
    song_query = request.args.get("song", "")
    singer_query = request.args.get("singer", "")
    sort_order = request.args.get("sort", "asc")
    df_all = df_from_db()
    if song_query:
        df_all = df_all[df_all["曲名"].fillna("").str.contains(song_query, case=False, na=False)]
    if singer_query:
        df_all = df_all[df_all["歌手名"].fillna("").str.contains(singer_query, case=False, na=False)]
    ascending = (sort_order == "asc")
    if not df_all.empty:
        df_all = df_all.sort_values("曲名", key=lambda s: s.map(ja_sort_key), ascending=ascending)
    ranking_data = {}
    if not df_all.empty:
        best_scores = df_all.groupby(["曲名", "ユーザー"], as_index=False).agg({"歌手名": "first", "スコア": "max", "日付": "max"})
        for song, group in best_scores.groupby("曲名"):
            ranking_data[song] = group.sort_values("スコア", ascending=False).head(3).to_dict(orient="records")
    return render_template("ranking.html", ranking_data=ranking_data, song_query=song_query, singer_query=singer_query, sort_order=sort_order)

@app.route("/update_ranking", methods=["POST"])
def update_ranking():
    song_query = request.form.get("song", "")
    singer_query = request.form.get("singer", "")
    sort_order = request.form.get("sort", "asc")
    total_inserted = 0
    for user, cookies in USER_COOKIES.items():
        if not cookies.get("scr_cdm"):
            continue
        df_new = fetch_damtomo_ai_scores(user, cookies)
        if not df_new.empty:
            inserted = insert_scores_from_df(df_new)
            total_inserted += inserted
            print(f"[update] {user}: inserted {inserted} rows")
    df_all = df_from_db()
    ranking_data = {}
    if not df_all.empty:
        best_scores = df_all.groupby(["曲名", "ユーザー"], as_index=False).agg({"歌手名": "first", "スコア": "max", "日付": "max"})
        for song, group in best_scores.groupby("曲名"):
            ranking_data[song] = group.sort_values("スコア", ascending=False).head(3).to_dict(orient="records")
    flash(f"新規 {total_inserted} 件を取り込みました", 'info')
    return render_template("ranking.html", ranking_data=ranking_data, song_query=song_query, singer_query=singer_query, sort_order=sort_order)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
