import os
import logging
from datetime import datetime, time
import pytz
import psycopg2
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telegram import Update
from apscheduler.schedulers.background import BackgroundScheduler

# --- 1. Environment Variables / Secrets ---
# Render se environment variable uthaayein
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
GROUP_CHAT_ID = os.getenv("GROUP_ID") 

if not BOT_TOKEN or not DATABASE_URL or not GROUP_CHAT_ID:
    logging.error("Environment variables BOT_TOKEN, DATABASE_URL, or GROUP_ID are missing.")
    # Agar secrets load nahin hote, toh aap yahan hardcode kar sakte hain test ke liye,
    # lekin production mein secrets use karna behtar hai.
    # Example:
    # BOT_TOKEN = "YOUR_BOT_TOKEN"
    # GROUP_CHAT_ID = -123456789 
    
# --- 2. Database Setup ---
def get_db_connection():
    """Connects to the PostgreSQL database using DATABASE_URL."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        return None

def setup_database():
    """Creates the 'members' table if it doesn't exist."""
    conn = get_db_connection()
    if conn is None:
        return

    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS members (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                submission_status TEXT,
                target_count INT DEFAULT 0,
                last_updated TIMESTAMP
            );
        """)
        conn.commit()
        logging.info("Database setup complete.")
    except Exception as e:
        logging.error(f"Error setting up database: {e}")
    finally:
        if conn:
            conn.close()

# --- 3. Database Functions ---
def add_member(user_id, username):
    """Adds a new member or updates an existing one."""
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
        logging.error(f"Error adding member: {e}")
        return False
    finally:
        if conn:
            conn.close()

def update_submission_status(user_id, status):
    """Updates a member's submission status."""
    conn = get_db_connection()
    if conn is None:
        return False
    
    try:
        cur = conn.cursor()
        if status == 'Completed':
             cur.execute("""
                UPDATE members SET submission_status = %s, target_count = target_count + 1, last_updated = NOW()
                WHERE user_id = %s;
            """, (status, user_id))
        else:
            cur.execute("""
                UPDATE members SET submission_status = %s, last_updated = NOW()
                WHERE user_id = %s;
            """, (status, user_id))
        conn.commit()
        return True
    except Exception as e:
        logging.error(f"Error updating status: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_all_members():
    """Fetches all members' data."""
    conn = get_db_connection()
    if conn is None:
        return []
    
    try:
        cur = conn.cursor()
        cur.execute("SELECT user_id, username, submission_status, target_count FROM members ORDER BY target_count DESC")
        return cur.fetchall()
    except Exception as e:
        logging.error(f"Error fetching members: {e}")
        return []
    finally:
        if conn:
            conn.close()

# --- 4. Telegram Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a message when the command /start is issued."""
    user = update.effective_user
    username = user.username if user.username else user.first_name
    add_member(user.id, username)

    await update.message.reply_html(
        f"Hello {user.first_name}!\n\nMain aapka daily target tracker bot hoon.\n"
        f"Subah **5-9 AM**: Initial target bhejien (photo, caption optional)\n"
        f"Raat **9-11 PM**: Completion proof bhejien (photo + caption: 'today target completed')"
    )

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Shows the current status of all members."""
    members = get_all_members()
    if not members:
        await update.message.reply_text("Abhi tak koi member register nahi hua hai.")
        return

    response = "🎯 **Target Tracking Status** 🎯\n\n"
    for user_id, username, status, count in members:
        response += f"👤 @{username} (Total: {count}): **{status}**\n"
        
    await update.message.reply_html(response)

async def handle_photo_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles photo messages and updates status."""
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else update.effective_user.first_name
    current_time = datetime.now(pytz.timezone('Asia/Kolkata')).time()

    add_member(user_id, username) # Ensure member exists

    # Define submission windows
    morning_start = time(5, 0)
    morning_end = time(9, 0)
    night_start = time(21, 0)
    night_end = time(23, 0)

    is_morning_window = morning_start <= current_time <= morning_end
    is_night_window = night_start <= current_time <= night_end
    
    if is_morning_window:
        update_submission_status(user_id, 'Planned')
        await update.message.reply_text("✅ Target Plan Received! Status updated to 'Planned'.")
    elif is_night_window:
        caption = update.message.caption.lower() if update.message.caption else ""
        if 'today target completed' in caption:
            update_submission_status(user_id, 'Completed')
            await update.message.reply_text("🔥 Target Completion Proof Received! Status updated to 'Completed'. Congratulations!")
        else:
            await update.message.reply_text("⚠️ Completion proof ke liye caption mein 'today target completed' zaroor likhein.")
    else:
        await update.message.reply_text("Submission window is closed. Morning: 5-9 AM (Planning), Night: 9-11 PM (Result).")


# --- 5. Scheduler Jobs ---
async def reset_daily_status(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Resets all users' status to Pending at 12:00 AM IST."""
    conn = get_db_connection()
    if conn is None:
        return
    
    try:
        cur = conn.cursor()
        cur.execute("UPDATE members SET submission_status = 'Pending'")
        conn.commit()
        
        # Send a message to the group
        await context.bot.send_message(
            chat_id=GROUP_CHAT_ID,
            text="⏰ **Daily Reset!** Sabhi members ka status 'Pending' par reset ho gaya hai. Naye targets ke liye taiyar ho jaaiye! 🎯"
        )
    except Exception as e:
        logging.error(f"Error resetting status: {e}")
    finally:
        if conn:
            conn.close()

async def send_reminder(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a reminder to users whose status is not 'Completed' at 9:30 PM IST."""
    members = get_all_members()
    pending_users = [f"@{m[1]}" for m in members if m[2] != 'Completed']
    
    if pending_users:
        reminder_text = "🔔 **Target Reminder!** 🔔\n\n"
        reminder_text += "Aapke paas completion proof bhejne ke liye sirf thoda samay bacha hai!\n"
        reminder_text += f"Pending: {', '.join(pending_users)}"
        
        await context.bot.send_message(
            chat_id=GROUP_CHAT_ID,
            text=reminder_text
        )

# --- 6. Main Application ---
def main() -> None:
    """Starts the bot."""
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    
    # 1. Database setup
    setup_database()
    
    # 2. Application setup
    application = Application.builder().token(BOT_TOKEN).build()

    # 3. Handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(MessageHandler(filters.PHOTO & filters.Chat(int(GROUP_CHAT_ID)), handle_photo_message))

    # 4. Scheduler
    scheduler = BackgroundScheduler(timezone='Asia/Kolkata')
    
    # Daily Status Reset at 12:00 AM IST
    scheduler.add_job(reset_daily_status, 'cron', hour=0, minute=0, name='Daily Status Reset')
    
    # Evening Reminder at 9:30 PM IST
    scheduler.add_job(send_reminder, 'cron', hour=21, minute=30, name='Evening Reminder')
    
    # Start the scheduler
    scheduler.start()
    logging.info("Scheduler started.")

    # 5. Run the bot
    logging.info("Bot is running...")
    application.run_polling(poll_interval=1.0)

if __name__ == "__main__":
    main()
