# main.py
import os
import logging
import asyncio
from datetime import datetime, date, time, timedelta
import pytz
import psycopg2
from psycopg2.extras import RealDictCursor
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telegram import Update
from apscheduler.schedulers.background import BackgroundScheduler

# ------------------------
# 1. Environment / Config
# ------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
try:
    GROUP_CHAT_ID = int(os.getenv("GROUP_ID"))
except (TypeError, ValueError):
    GROUP_CHAT_ID = None
    logging.error("GROUP_ID environment variable is missing or invalid.")

# Optional: thread id (for Telegram forum threads). If provided, messages will send to thread.
try:
    THREAD_ID = int(os.getenv("THREAD_ID")) if os.getenv("THREAD_ID") else None
except (TypeError, ValueError):
    THREAD_ID = None

# global app reference used by scheduled jobs to send messages
telegram_bot_app = None

# Timezone
IST = pytz.timezone("Asia/Kolkata")

# ------------------------
# 2. Logging
# ------------------------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# ------------------------
# 3. Database helpers
# ------------------------
def get_db_connection():
    """Return a new DB connection or None on failure."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        logging.error(f"DB connect error: {e}")
        return None

def setup_database():
    """Create members table and add columns if missing."""
    conn = get_db_connection()
    if conn is None:
        logging.error("Cannot setup DB: connection failed.")
        return
    try:
        cur = conn.cursor()
        # Create table with columns for points, streak and last_completed_date
        cur.execute("""
            CREATE TABLE IF NOT EXISTS members (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                submission_status TEXT,
                target_count INT DEFAULT 0,
                points INT DEFAULT 0,
                streak INT DEFAULT 0,
                last_completed_date DATE,
                last_updated TIMESTAMP
            );
        """)
        conn.commit()
        logging.info("DB setup complete (members table ensured).")
    except Exception as e:
        logging.error(f"Error in setup_database: {e}")
    finally:
        conn.close()

# ------------------------
# 4. Member operations
# ------------------------
def add_member(user_id: int, username: str):
    """Add or update basic member row (keeps existing points/streak)."""
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO members (user_id, username, submission_status, last_updated)
            VALUES (%s, %s, 'Pending', NOW())
            ON CONFLICT (user_id) DO UPDATE SET
                username = EXCLUDED.username,
                last_updated = NOW();
        """, (user_id, username))
        conn.commit()
        return True
    except Exception as e:
        logging.error(f"Error in add_member: {e}")
        return False
    finally:
        conn.close()

def fetch_member(user_id: int):
    conn = get_db_connection()
    if conn is None:
        return None
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM members WHERE user_id = %s", (user_id,))
        return cur.fetchone()
    except Exception as e:
        logging.error(f"Error fetch_member: {e}")
        return None
    finally:
        conn.close()

def update_submission_status(user_id: int, status: str):
    """
    Generic update: sets submission_status and last_updated.
    Use mark_completed() for Completed status (handles points/streak).
    """
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE members SET submission_status = %s, last_updated = NOW()
            WHERE user_id = %s;
        """, (status, user_id))
        conn.commit()
        return True
    except Exception as e:
        logging.error(f"Error update_submission_status: {e}")
        return False
    finally:
        conn.close()

def mark_completed(user_id: int):
    """
    Called when user completes today's target.
    Logic:
      - If last_completed_date is yesterday => streak +=1 and bonus applied
      - Else if last_completed_date is today => do not double count
      - Else (gap) => streak = 1
      - Points: +10 for completion + (if consecutive) +5 bonus
      - target_count +=1
      - update last_completed_date to today
      - set submission_status to 'Completed'
    """
    today = datetime.now(IST).date()
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT points, streak, last_completed_date FROM members WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
        if not row:
            # If the user isn't present, create and then mark
            cur.execute("""
                INSERT INTO members (user_id, username, submission_status, target_count, points, streak, last_completed_date, last_updated)
                VALUES (%s, %s, 'Completed', 1, 10, 1, %s, NOW())
                ON CONFLICT (user_id) DO NOTHING;
            """, (user_id, "Unknown", today))
            conn.commit()
            return True

        existing_points = row['points'] or 0
        existing_streak = row['streak'] or 0
        last_date = row['last_completed_date']

        # Prevent double awarding if already completed today
        if last_date == today:
            # still update status and last_updated
            cur.execute("""
                UPDATE members SET submission_status = 'Completed', last_updated = NOW()
                WHERE user_id = %s;
            """, (user_id,))
            conn.commit()
            return True

        # Determine streak
        yesterday = today - timedelta(days=1)
        bonus = 0
        if last_date == yesterday:
            new_streak = existing_streak + 1
            bonus = 5  # streak bonus
        else:
            new_streak = 1

        points_awarded = 10 + bonus
        new_points = existing_points + points_awarded

        cur.execute("""
            UPDATE members
            SET submission_status = 'Completed',
                target_count = target_count + 1,
                points = %s,
                streak = %s,
                last_completed_date = %s,
                last_updated = NOW()
            WHERE user_id = %s;
        """, (new_points, new_streak, today, user_id))
        conn.commit()
        return True
    except Exception as e:
        logging.error(f"Error mark_completed: {e}")
        return False
    finally:
        conn.close()

def get_all_members(order_by_points=True):
    """
    Fetch all members with necessary fields.
    Default ordering: points DESC, streak DESC, username ASC
    """
    conn = get_db_connection()
    if conn is None:
        return []
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT user_id, username, submission_status, target_count, points, streak, last_completed_date
            FROM members
        """)
        rows = cur.fetchall()
        # Sort in Python so we control tie-breaks exactly
        rows_sorted = sorted(rows, key=lambda r: ((r['points'] or 0), (r['streak'] or 0)), reverse=True)
        return rows_sorted
    except Exception as e:
        logging.error(f"Error get_all_members: {e}")
        return []
    finally:
        conn.close()

def apply_missed_deductions_and_reset():
    """
    For members who are not 'Completed' for TODAY:
     - subtract 5 points (min 0)
     - reset streak to 0
     - set submission_status to 'Missed' (so we know)
    Returns tuple (missed_usernames_list, updated_count)
    """
    today = datetime.now(IST).date()
    conn = get_db_connection()
    if conn is None:
        return [], 0
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        # fetch all members
        cur.execute("SELECT user_id, username, submission_status, points FROM members")
        rows = cur.fetchall()
        missed = []
        updated = 0
        for r in rows:
            uid = r['user_id']
            uname = r['username']
            status = r['submission_status']
            pts = r['points'] or 0
            # If they haven't completed today (status not Completed OR last_completed_date != today)
            cur2 = conn.cursor(cursor_factory=RealDictCursor)
            cur2.execute("SELECT last_completed_date FROM members WHERE user_id = %s", (uid,))
            ld = cur2.fetchone()
            last_completed_date = ld['last_completed_date'] if ld else None
            completed_today = (last_completed_date == today)
            if not completed_today:
                # apply deduction and reset streak
                new_points = max(0, pts - 5)
                cur.execute("""
                    UPDATE members
                    SET points = %s, streak = 0, submission_status = 'Missed', last_updated = NOW()
                    WHERE user_id = %s;
                """, (new_points, uid))
                missed.append(uname if uname else str(uid))
                updated += 1
        conn.commit()
        return missed, updated
    except Exception as e:
        logging.error(f"Error apply_missed_deductions_and_reset: {e}")
        return [], 0
    finally:
        conn.close()

# ------------------------
# 5. Telegram handlers
# ------------------------
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    username = user.username if user.username else user.first_name
    add_member(user.id, username)
    await update.message.reply_html(
        f"Hello {user.first_name}!\n\nMain aapka daily target tracker bot hoon.\n"
        f"Subah 5-9 AM: Initial plan bhejien (photo/caption optional)\n"
        f"Raat 9-11 PM: Proof bhejien with caption 'today target completed' or use /done.\n"
        f"Use /status to view leaderboard and /done to mark completion."
    )

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    members = get_all_members()
    if not members:
        await update.message.reply_text("Abhi tak koi member register nahi hua hai.")
        return
    response = "🎯 **Target Tracking Status (Live)** 🎯\n\n"
    for idx, m in enumerate(members, start=1):
        username = m['username'] or str(m['user_id'])
        status = m['submission_status'] or "Pending"
        pts = m['points'] or 0
        streak = m['streak'] or 0
        response += f"{idx}. @{username} — {pts} pts | 🔥 Streak: {streak} | {status}\n"
    await update.message.reply_text(response)

async def done_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Mark user as done via /done command (usable in group)."""
    user = update.effective_user
    user_id = user.id
    username = user.username if user.username else user.first_name

    # ensure member exists
    add_member(user_id, username)

    success = mark_completed(user_id)
    if success:
        # fetch new points & streak to show in the reply
        member = fetch_member(user_id)
        pts = member['points'] or 0
        streak = member['streak'] or 0
        await update.message.reply_text(f"🔥 Nice! @{username} marked as Completed. +10 pts {'+5 streak bonus' if streak>1 else ''}\nTotal: {pts} pts | 🔥 Streak: {streak} days")
    else:
        await update.message.reply_text("Kuch gadbad ho gayi. Dobara try karo.")

async def handle_photo_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    user_id = user.id
    username = user.username if user.username else user.first_name

    # ensure group-only
    if update.effective_chat.id != GROUP_CHAT_ID:
        return

    current_time = datetime.now(IST).time()
    add_member(user_id, username)

    morning_start = time(5, 0)
    morning_end = time(9, 0)
    night_start = time(21, 0)
    night_end = time(23, 0)

    is_morning = morning_start <= current_time <= morning_end
    is_night = night_start <= current_time <= night_end

    if is_morning:
        update_submission_status(user_id, "Planned")
        await update.message.reply_text("✅ Target Plan Received! Status updated to 'Planned'.")
    elif is_night:
        caption = (update.message.caption or "").lower()
        if 'today target completed' in caption or 'today target complete' in caption or 'target completed' in caption:
            ok = mark_completed(user_id)
            if ok:
                member = fetch_member(user_id)
                await update.message.reply_text(
                    f"🔥 Target proof received, @{username}! Status set to Completed. Total: {member['points']} pts | 🔥 Streak: {member['streak']} days"
                )
            else:
                await update.message.reply_text("Kuch error hua while marking completion. Try /done.")
        else:
            await update.message.reply_text("⚠️ Proof ke liye caption mein 'today target completed' likhein (ya /done use karein).")
    else:
        # outside submission windows
        await update.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"@{username}, Submission window closed. Morning: 5-9 AM, Night: 9-11 PM."
        )

# ------------------------
# 6. Scheduler jobs (async wrappers)
# ------------------------
async def send_leaderboard_job():
    """Sends full-members leaderboard at 11:01 PM IST (mixed Hindi-English tone)."""
    global telegram_bot_app
    if not telegram_bot_app:
        logging.info("Bot app not ready — skipping leaderboard job.")
        return

    members = get_all_members()
    if not members:
        await telegram_bot_app.bot.send_message(chat_id=GROUP_CHAT_ID, text="Leaderboard: koi member nahi mila.")
        return

    # Build message: full members list, sorted by points then streak (already sorted)
    today_str = datetime.now(IST).strftime("%-d %b %Y")
    header = f"🔥 **Vaibhav’s Inferno Tracker — Leaderboard ({today_str})** 🔥\n\n"
    body = ""
    for idx, m in enumerate(members, start=1):
        uname = m['username'] or str(m['user_id'])
        pts = m['points'] or 0
        streak = m['streak'] or 0
        status = m['submission_status'] or "Pending"
        # Mixed Hindi-English line
        if status == 'Completed':
            line = f"#{idx} 🏆 @{uname} — {pts} pts | 🔥 Streak: {streak} days — Aaj ka kaam perfect! ✅\n"
        elif status == 'Missed':
            line = f"#{idx} ⚠️ @{uname} — {pts} pts | 🔻 Streak reset — Aaj missed. Chal next time!\n"
        elif status == 'Planned':
            line = f"#{idx} 🔜 @{uname} — {pts} pts | 🔁 Planned — Jaldi proof bhejo!\n"
        else:
            line = f"#{idx} ❗ @{uname} — {pts} pts | Streak: {streak} — Abhi pending.\n"
        body += line

    footer = "\n🏁 Keep pushing — kal fir se full josh! 💪\n(Leaderboard updates everyday 11:01 PM IST)"
    final_msg = header + body + footer

    # If thread_id provided and non-zero, Telegram supports sending to a thread by specifying message_thread_id (only for forum supergroups)
    send_kwargs = {"chat_id": GROUP_CHAT_ID, "text": final_msg, "parse_mode": "Markdown"}
    if THREAD_ID:
        send_kwargs["message_thread_id"] = THREAD_ID

    await telegram_bot_app.bot.send_message(**send_kwargs)
    logging.info("Leaderboard sent.")

async def nightly_process_job():
    """
    At 23:05-ish we:
     - apply missed deductions for those who didn't complete today
     - then send leaderboard (so leaderboard reflects deductions)
    """
    global telegram_bot_app
    if not telegram_bot_app:
        logging.info("Bot not ready — skipping nightly_process_job.")
        return

    # Apply missed deductions & reset streak for missed users
    missed_list, updated = apply_missed_deductions_and_reset()
    if missed_list:
        # Notify group about deductions (short message)
        text = f"⚠️ Missed submissions detected for {len(missed_list)} members — -5 pts and streak reset applied.\nPending: {', '.join(['@'+m for m in missed_list])}"
        send_kwargs = {"chat_id": GROUP_CHAT_ID, "text": text}
        if THREAD_ID:
            send_kwargs["message_thread_id"] = THREAD_ID
        await telegram_bot_app.bot.send_message(**send_kwargs)

    # Small delay then send leaderboard
    await asyncio.sleep(1)
    await send_leaderboard_job()

async def reset_daily_status_job():
    """At 00:00 AM IST — reset everyone's submission_status to 'Pending' for the new day."""
    global telegram_bot_app
    if not telegram_bot_app:
        logging.info("Bot not ready — skipping reset job.")
        return
    conn = get_db_connection()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("UPDATE members SET submission_status = 'Pending', last_updated = NOW()")
        conn.commit()
        txt = "⏰ **Daily Reset!** Sabka status ab 'Pending' par set kar diya gaya hai. Naye targets bhejne shuru karo! 🎯"
        send_kwargs = {"chat_id": GROUP_CHAT_ID, "text": txt, "parse_mode": "Markdown"}
        if THREAD_ID:
            send_kwargs["message_thread_id"] = THREAD_ID
        await telegram_bot_app.bot.send_message(**send_kwargs)
    except Exception as e:
        logging.error(f"Error in reset_daily_status_job: {e}")
    finally:
        conn.close()

async def evening_reminder_job():
    """9:30 PM IST reminder for pending users."""
    global telegram_bot_app
    if not telegram_bot_app:
        logging.info("Bot not ready — skipping reminder job.")
        return
    members = get_all_members()
    pending = [f"@{m['username']}" for m in members if m['submission_status'] != 'Completed']
    if pending:
        text = "🔔 **Target Reminder!** 🔔\nAaj raat proof bhejna na bhoolo!\nPending: " + ", ".join(pending)
        send_kwargs = {"chat_id": GROUP_CHAT_ID, "text": text, "parse_mode": "Markdown"}
        if THREAD_ID:
            send_kwargs["message_thread_id"] = THREAD_ID
        await telegram_bot_app.bot.send_message(**send_kwargs)

# ------------------------
# 7. Main app and scheduler
# ------------------------
def main():
    global telegram_bot_app

    if not BOT_TOKEN or not DATABASE_URL or GROUP_CHAT_ID is None:
        logging.error("Missing required environment variables. Exiting.")
        return

    # DB setup
    setup_database()

    # Application setup
    application = Application.builder().token(BOT_TOKEN).build()
    telegram_bot_app = application  # set global for jobs

    # Handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("done", done_command))
    application.add_handler(MessageHandler(filters.PHOTO & filters.Chat(GROUP_CHAT_ID), handle_photo_message))

    # Scheduler (BackgroundScheduler). We'll schedule wrappers that create asyncio tasks for coroutines.
    scheduler = BackgroundScheduler(timezone="Asia/Kolkata")

    # 1) Evening reminder: 21:30 IST
    scheduler.add_job(lambda: asyncio.create_task(evening_reminder_job()), "cron", hour=21, minute=30, id="evening_reminder")

    # 2) Leaderboard job: 23:01 IST - first apply deductions & then send leaderboard (we'll use a wrapper)
    # We'll run the nightly process at 23:01 (apply deductions + send leaderboard)
    scheduler.add_job(lambda: asyncio.create_task(nightly_process_job()), "cron", hour=23, minute=1, id="nightly_process")

    # 3) Daily reset at 00:00 IST
    scheduler.add_job(lambda: asyncio.create_task(reset_daily_status_job()), "cron", hour=0, minute=0, id="daily_reset")

    scheduler.start()
    logging.info("Scheduler started with jobs: evening_reminder (21:30), nightly_process (23:01), daily_reset (00:00).")

    # Run bot (polling). stop_signals=None recommended for containerized environments on Render.
    logging.info("Bot starting polling...")
    application.run_polling(poll_interval=1.0, stop_signals=None)

if __name__ == "__main__":
    main()
