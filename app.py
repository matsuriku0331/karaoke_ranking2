import os
import logging
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, session, flash
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import UniqueConstraint, text
from sqlalchemy.exc import IntegrityError, OperationalError

# ---- Flask + DB setup ----
app = Flask(__name__)
app.logger.setLevel(logging.INFO)

# DATABASE_URL 設定
DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL:
    uri = DATABASE_URL.replace("postgres://", "postgresql://")
    if uri.startswith("postgresql://") and "sslmode=" not in uri:
        uri += ("&" if "?" in uri else "?") + "sslmode=require"
    app.config["SQLALCHEMY_DATABASE_URI"] = uri
else:
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///scores.db"

app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_pre_ping": True,
    "pool_recycle": 300,
    "pool_timeout": 30,
}
app.secret_key = os.environ.get("SECRET_KEY", "dev_key")

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


with app.app_context():
    db.create_all()


@app.teardown_appcontext
def shutdown_session(exception=None):
    db.session.remove()


# ---- Config / constants ----
AI_SCORE_URL = "https://www.clubdam.com/app/damtomo/scoring/GetScoringAiListXML.do"

USER_COOKIES = {
    "まつりく": {
        "dam-uid": "...",
        "scr_cdm": "...",
        "scr_dt": "...",
        "webmember": "1",
        "wm_ac": "...",
        "wm_dm": "..."
    },
    "えす": {...},
    "こんけあ": {...},
}


# ---- Helpers ----
def fetch_damtomo_ai_scores(username, cookies, max_pages=40):
    all_scores = []
    for page in range(1, max_pages + 1):
        params = {"cdmCardNo": cookies.get("scr_cdm", ""), "pageNo": page, "detailFlg": 0}
        try:
            res = requests.get(AI_SCORE_URL, cookies=cookies, params=params, timeout=15)
            res.raise_for_status()
        except Exception as e:
            print(f"[fetch] {username} page {page} request failed: {e}")
            break

        try:
            root = ET.fromstring(res.content)
        except Exception as e:
            print(f"[fetch] {username} page {page} XML parse failed: {e}")
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
    session = db.session
    for _, r in df_new.iterrows():
        if pd.isna(r["日付"]) or pd.isna(r["スコア"]):
            continue
        song, singer, user, score_val, date_val = (
            str(r["曲名"]),
            str(r["歌手名"]) if not pd.isna(r["歌手名"]) else None,
            str(r["ユーザー"]),
            float(r["スコア"]),
            r["日付"].to_pydatetime() if hasattr(r["日付"], "to_pydatetime") else r["日付"],
        )
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


def df_from_db():
    def _read_all():
        return db.session.query(Score).all()

    try:
        rows = _read_all()
    except OperationalError as e:
        app.logger.warning("DB OperationalError: %s. Retrying...", e.__class__.__name__)
        try:
            db.session.rollback()
        except Exception:
            pass
        try:
            db.engine.dispose()
        except Exception:
            pass
        rows = _read_all()

    data = [
        {"曲名": r.song, "歌手名": r.singer, "ユーザー": r.user, "スコア": r.score, "日付": r.date}
        for r in rows
    ]
    if not data:
        return pd.DataFrame(columns=["曲名", "歌手名", "ユーザー", "スコア", "日付"])
    return pd.DataFrame(data)


# ---- Routes ----
@app.route("/")
def home():
    return render_template("home.html")


@app.route("/ranking")
def ranking():
    song_query = request.args.get("song", "")
    singer_query = request.args.get("singer", "")

    df_all = df_from_db()
    if song_query:
        df_all = df_all[df_all["曲名"].str.contains(song_query, na=False)]
    if singer_query:
        df_all = df_all[df_all["歌手名"].str.contains(singer_query, na=False)]

    df_all = df_all.sort_values("曲名", ascending=True)

    ranking_data = {}
    if not df_all.empty:
        best_scores = df_all.groupby(["曲名", "ユーザー"], as_index=False).agg(
            {"歌手名": "first", "スコア": "max", "日付": "first"}
        )
        for song, group in best_scores.groupby("曲名"):
            ranking_data[song] = (
                group.sort_values("スコア", ascending=False)
                .head(3)
                .to_dict(orient="records")
            )

    return render_template("ranking.html", ranking_data=ranking_data,
                           song_query=song_query, singer_query=singer_query)


@app.route("/update_ranking", methods=["POST"])
def update_ranking():
    for user, cookies in USER_COOKIES.items():
        df_new = fetch_damtomo_ai_scores(user, cookies)
        if not df_new.empty:
            insert_scores_from_df(df_new)
    return redirect(url_for("ranking"))


# ---- Health Check ----
@app.get("/health")
def health():
    return {"ok": True}, 200


@app.get("/health/db")
def health_db():
    try:
        db.session.execute(text("SELECT 1"))
        return {"db": "ok"}, 200
    except Exception as e:
        return {"db": "ng", "error": str(e)}, 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)