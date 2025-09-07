import os
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from flask import Flask, render_template, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import UniqueConstraint
from sqlalchemy.exc import IntegrityError
from datetime import datetime

# ---- Flask + DB setup ----
app = Flask(__name__)

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
        return {
            "曲名": self.song,
            "歌手名": self.singer,
            "ユーザー": self.user,
            "スコア": self.score,
            "日付": self.date
        }

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

# ---- Helper: fetch DAM★とも AI scores (multi-page) ----
def fetch_damtomo_ai_scores(username, cookies, max_pages=40):
    all_scores = []
    for page in range(1, max_pages + 1):
        params = {
            "cdmCardNo": cookies.get("scr_cdm", ""),
            "pageNo": page,
            "detailFlg": 0
        }
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

# ---- Helper: insert DataFrame rows into DB (skip duplicates) ----
def insert_scores_from_df(df_new):
    if df_new.empty:
        return 0
    df_new = df_new.copy()
    df_new["日付"] = pd.to_datetime(df_new["日付"], errors="coerce")
    inserted = 0

    session = db.session
    for _, r in df_new.iterrows():
        if pd.isna(r["日付"]) or pd.isna(r["スコア"]):
            continue
        song = str(r["曲名"])
        singer = str(r["歌手名"]) if not pd.isna(r["歌手名"]) else None
        user = str(r["ユーザー"])
        score_val = float(r["スコア"])
        date_val = r["日付"].to_pydatetime() if hasattr(r["日付"], "to_pydatetime") else r["日付"]

        exists = session.query(Score).filter_by(song=song, user=user, date=date_val).first()
        if exists:
            continue

        s = Score(song=song, singer=singer, user=user, score=score_val, date=date_val)
        session.add(s)
        try:
            session.commit()
            inserted += 1
        except IntegrityError:
            session.rollback()
        except Exception as e:
            session.rollback()
            print(f"[insert] unexpected error inserting {song}/{user}/{date_val}: {e}")
    return inserted

# ---- Helper: read all data from DB into DataFrame ----
def df_from_db():
    rows = db.session.query(Score).all()
    data = []
    for r in rows:
        data.append({
            "曲名": r.song,
            "歌手名": r.singer,
            "ユーザー": r.user,
            "スコア": r.score,
            "日付": r.date
        })
    if not data:
        return pd.DataFrame(columns=["曲名", "歌手名", "ユーザー", "スコア", "日付"])
    return pd.DataFrame(data)

# ---- Routes ----
@app.route("/", methods=["GET"])
def home():
    return render_template("home.html")

@app.route("/ranking", methods=["GET"])
def ranking():
    # 検索パラメータ
    song_query = request.args.get("song", "")
    singer_query = request.args.get("singer", "")
    # あいうえお順は廃止（ご要望）→ sort_order は無視

    # DB から読み出し
    df_all = df_from_db()

    # ② 平均点（全レコードからユーザー毎平均）
    user_averages = {}
    # ②’ 1位取得数
    first_place_counts = {}
    # ③’ 個人ごとの合計曲数（ユニーク）
    user_unique_song_counts = {}
    # ③’’ 全体の合計曲数（ユニーク）
    total_unique_songs = 0

    if not df_all.empty:
        # ユーザー平均
        user_averages = df_all.groupby("ユーザー")["スコア"].mean().round(2).to_dict()
        # 個人ユニーク曲数
        user_unique_song_counts = df_all.groupby("ユーザー")["曲名"].nunique().to_dict()
        # 全体ユニーク曲数
        total_unique_songs = int(df_all["曲名"].nunique())

    # フィルタリング（部分一致）
    if song_query:
        df_all = df_all[df_all["曲名"].fillna("").str.contains(song_query, case=False, na=False)]
    if singer_query:
        df_all = df_all[df_all["歌手名"].fillna("").str.contains(singer_query, case=False, na=False)]

    # ③：曲×ユーザーで「最高点の“達成時”の行」を抽出（同点なら最初に達成した日）
    best_rows = pd.DataFrame(columns=["曲名", "ユーザー", "歌手名", "スコア", "日付"])
    if not df_all.empty:
        ordered = df_all.sort_values(["スコア", "日付"], ascending=[False, True])
        best_rows = (
            ordered.groupby(["曲名", "ユーザー"], as_index=False)
                   .first()[["曲名", "ユーザー", "歌手名", "スコア", "日付"]]
        )

    # ①：曲ごとにTop3を作りつつ「曲の並び」を1位スコア高い順に
    ranking_list = []  # [{song, top_score, records(list)}]
    if not best_rows.empty:
        for song, group in best_rows.groupby("曲名"):
            # その曲の「ユーザー別最高」をスコア降順・日付昇順で並び替え→Top3
            g_sorted = group.sort_values(["スコア", "日付"], ascending=[False, True])
            top3 = g_sorted.head(3).to_dict(orient="records")
            top_score = float(g_sorted.iloc[0]["スコア"])
            ranking_list.append({"song": song, "top_score": top_score, "records": top3, "singer": g_sorted.iloc[0]["歌手名"]})

            # ②：1位取得者カウント（同点時は“最初に達成した人”を1名採用）
            winner = g_sorted.iloc[0]["ユーザー"]
            first_place_counts[winner] = first_place_counts.get(winner, 0) + 1

        # 曲の並び順：1位スコアの降順
        ranking_list.sort(key=lambda x: x["top_score"], reverse=True)

    return render_template(
        "ranking.html",
        ranking_list=ranking_list,          # ← dictではなく順序付きリストを渡す
        user_averages=user_averages,
        first_place_counts=first_place_counts,
        user_unique_song_counts=user_unique_song_counts,
        total_unique_songs=total_unique_songs,
        song_query=song_query,
        singer_query=singer_query,
    )

@app.route("/update_ranking", methods=["POST"])
def update_ranking():
    # 検索条件保持（フォームの hidden から）
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

    # 更新後も ranking() と同じ処理で再描画
    df_all = df_from_db()

    user_averages = {}
    first_place_counts = {}
    user_unique_song_counts = {}
    total_unique_songs = 0

    if not df_all.empty:
        user_averages = df_all.groupby("ユーザー")["スコア"].mean().round(2).to_dict()
        user_unique_song_counts = df_all.groupby("ユーザー")["曲名"].nunique().to_dict()
        total_unique_songs = int(df_all["曲名"].nunique())

    if song_query:
        df_all = df_all[df_all["曲名"].fillna("").str.contains(song_query, case=False, na=False)]
    if singer_query:
        df_all = df_all[df_all["歌手名"].fillna("").str.contains(singer_query, case=False, na=False)]

    best_rows = pd.DataFrame(columns=["曲名", "ユーザー", "歌手名", "スコア", "日付"])
    if not df_all.empty:
        ordered = df_all.sort_values(["スコア", "日付"], ascending=[False, True])
        best_rows = (
            ordered.groupby(["曲名", "ユーザー"], as_index=False)
                   .first()[["曲名", "ユーザー", "歌手名", "スコア", "日付"]]
        )

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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
