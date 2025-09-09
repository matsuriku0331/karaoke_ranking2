import os
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from flask import Flask, render_template, request, redirect, url_for, session, flash
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import UniqueConstraint, text
from sqlalchemy.exc import IntegrityError, OperationalError
from datetime import datetime
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

# ---- Flask + DB setup ----
app = Flask(__name__)

# セッション＆管理
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret-change-me")
ADMIN_PASS = os.environ.get("ADMIN_PASS", None)  # 管理者用共有パスワード

# Render の接続文字列（postgres:// → postgresql:// 置換 & sslmode=require 付与）
DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL:
    dburl = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    # sslmode=require がついていない場合は付与
    try:
        parsed = urlparse(dburl)
        q = dict(parse_qsl(parsed.query))
        if "sslmode" not in q:
            q["sslmode"] = "require"
        dburl = urlunparse(parsed._replace(query=urlencode(q)))
    except Exception:
        # パースに失敗してもそのまま使う
        pass
    app.config["SQLALCHEMY_DATABASE_URI"] = dburl
else:
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///scores.db"

app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# ★ コネクションプールの堅牢化
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_pre_ping": True,   # 死活監視で切れた接続を自動再取得
    "pool_recycle": 280,     # 300秒未満でリサイクル（LB等のアイドル切断対策）
    "pool_size": 5,
    "max_overflow": 10,
}

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

# アプリ終了時に確実にセッションを閉じる
@app.teardown_appcontext
def shutdown_session(exception=None):
    try:
        db.session.remove()
    except Exception:
        pass

# ---- Jinja フィルタ ----
@app.template_filter("fmtdate")
def fmtdate(value, fmt="%Y-%m-%d"):
    """
    日付を安全に書式化。None/文字列/Datetime どれでもOK。
    """
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
        s = str(value)
        return s[:10]

# ---- Config / constants ----
AI_SCORE_URL = "https://www.clubdam.com/app/damtomo/scoring/GetScoringAiListXML.do"

# 環境変数からクッキー値を読む（安全運用）
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
def fetch_damtomo_ai_scores(username, cookies, max_pages=10):
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

def safe_commit(session_db):
    """
    commitでOperationalError(接続切断)が出たときに、1回だけプール破棄→再試行する。
    """
    try:
        session_db.commit()
    except OperationalError as e:
        # 接続切断など
        session_db.rollback()
        try:
            db.engine.dispose()  # 既存プールを破棄して新規接続へ
        except Exception:
            pass
        # 再試行
        session_db.commit()

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

        # 既存チェック
        try:
            exists = session_db.query(Score).filter_by(song=song, user=user, date=date_val).first()
        except OperationalError:
            session_db.rollback()
            try:
                db.engine.dispose()
            except Exception:
                pass
            exists = session_db.query(Score).filter_by(song=song, user=user, date=date_val).first()

        if exists:
            continue

        s = Score(song=song, singer=singer, user=user, score=score_val, date=date_val)
        session_db.add(s)
        try:
            safe_commit(session_db)
            inserted += 1
        except IntegrityError:
            session_db.rollback()
        except OperationalError as e:
            # 2回目の失敗は諦めて次レコードへ
            session_db.rollback()
            print(f"[insert] OperationalError after retry {song}/{user}/{date_val}: {e}")
        except Exception as e:
            session_db.rollback()
            print(f"[insert] unexpected error inserting {song}/{user}/{date_val}: {e}")
    return inserted

def df_from_db():
    try:
        rows = db.session.query(Score).all()
    except OperationalError:
        db.session.rollback()
        try:
            db.engine.dispose()
        except Exception:
            pass
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

# ---- Public Routes ----
@app.route("/", methods=["GET"])
def home():
    return render_template("home.html")

@app.route("/ranking", methods=["GET"])
def ranking():
    song_query = request.args.get("song", "")
    singer_query = request.args.get("singer", "")

    df_all = df_from_db()

    user_averages = {}
    first_place_counts = {}
    third_place_counts = {}
    user_total_records = {}

    if not df_all.empty:
        # 平均は3桁表示
        user_averages = df_all.groupby("ユーザー")["スコア"].mean().round(3).to_dict()
        user_total_records = df_all.groupby("ユーザー").size().to_dict()

    filtered = df_all.copy()
    if song_query:
        filtered = filtered[filtered["曲名"].fillna("").str.contains(song_query, case=False, na=False)]
    if singer_query:
        filtered = filtered[filtered["歌手名"].fillna("").str.contains(singer_query, case=False, na=False)]

    best_rows = pd.DataFrame(columns=["曲名", "ユーザー", "歌手名", "スコア", "日付"])
    if not filtered.empty:
        ordered = filtered.sort_values(["スコア", "日付"], ascending=[False, True])
        best_rows = (
            ordered.groupby(["曲名", "ユーザー"], as_index=False)
                   .first()[["曲名", "ユーザー", "歌手名", "スコア", "日付"]]
        )

    ranking_list = []
    first_place_counts = {}
    third_place_counts = {}
    if not best_rows.empty:
        for song, group in best_rows.groupby("曲名"):
            g_sorted = group.sort_values(["スコア", "日付"], ascending=[False, True])
            top3_df = g_sorted.head(3)
            top3 = top3_df.to_dict(orient="records")
            top_score = float(g_sorted.iloc[0]["スコア"])
            ranking_list.append({
                "song": song,
                "top_score": top_score,
                "records": top3,
                "singer": g_sorted.iloc[0]["歌手名"]
            })
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
        user_averages=user_averages,
        first_place_counts=first_place_counts,
        third_place_counts=third_place_counts,
        user_total_records=user_total_records,
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
            print(f("[update] {user}: inserted {inserted} rows"))
    return redirect(url_for("ranking", song=song_query, singer=singer_query))

# ---- User History ----
@app.route("/user/<username>", methods=["GET"])
def user_history(username):
    sort = request.args.get("sort", "recent")
    song_query = request.args.get("song", "").strip()
    singer_query = request.args.get("singer", "").strip()
    # ページネーション
    try:
        page = max(int(request.args.get("page", 1)), 1)
    except Exception:
        page = 1
    try:
        per = int(request.args.get("per", 50))
    except Exception:
        per = 50
    per = min(max(per, 10), 200)  # 10〜200

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
        df = df.iloc[start:end]
        records = df.to_dict(orient="records")

    total_pages = (total + per - 1) // per if (per and total) else 1
    return render_template("user_history.html",
                           username=username, records=records, total=total,
                           sort=sort, song_query=song_query, singer_query=singer_query,
                           page=page, per=per, total_pages=total_pages)

# ---- User third rank ----
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

# ---- All users history ----
@app.route("/history/all", methods=["GET"])
def all_history():
    sort = request.args.get("sort", "recent")
    song_query = request.args.get("song", "").strip()
    singer_query = request.args.get("singer", "").strip()
    # ページネーション
    try:
        page = max(int(request.args.get("page", 1)), 1)
    except Exception:
        page = 1
    try:
        per = int(request.args.get("per", 50))
    except Exception:
        per = 50
    per = min(max(per, 10), 200)  # 10〜200

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
        df = df.iloc[start:end]
        records = df.to_dict(orient="records")

    total_pages = (total + per - 1) // per if (per and total) else 1
    return render_template("all_history.html",
                           records=records, total=total, sort=sort,
                           song_query=song_query, singer_query=singer_query,
                           page=page, per=per, total_pages=total_pages)

# ---- Admin ----
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
        safe_commit(db.session)
        flash("1件追加しました。", "info")
    except IntegrityError:
        db.session.rollback()
        flash("同じ（曲名・ユーザー・日時）のデータが既に存在します。", "error")
    except OperationalError as e:
        db.session.rollback()
        flash(f"接続エラーのため追加に失敗しました: {e}", "error")
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

    try:
        row = Score.query.filter_by(song=song, user=user, date=date_val).first()
    except OperationalError:
        db.session.rollback()
        try:
            db.engine.dispose()
        except Exception:
            pass
        row = Score.query.filter_by(song=song, user=user, date=date_val).first()

    if not row:
        flash("該当レコードが見つかりません（曲名・ユーザー・日時の完全一致で検索）。", "error")
        return redirect(url_for("admin"))

    try:
        db.session.delete(row)
        safe_commit(db.session)
        flash("1件削除しました。", "info")
    except Exception as e:
        db.session.rollback()
        flash(f"削除でエラーが発生しました: {e}", "error")
    return redirect(url_for("admin"))

# ---- 起動 ----
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)