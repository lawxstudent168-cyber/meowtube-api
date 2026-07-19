import os
import asyncio
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from telethon import TelegramClient
from telethon.sessions import StringSession
from dotenv import load_dotenv

# ==========================================
# 1. 環境變數與初始化設定
# ==========================================
load_dotenv()

# 變數名稱已經修改為與 Render 上一模一樣
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
SESSION_STRING = os.getenv("SESSION_STRING", "")

# 確保群組 ID 是整數，且名稱對應 Render 的 KEY
PUBLIC_CHAT_ID = int(os.getenv("TARGET_GROUP_ID", 0))
SECRET_CHAT_ID = int(os.getenv("SECRET_TELEGRAM_CHAT_ID", 0))

# 初始化 FastAPI
app = FastAPI(title="Meowtube API")

# 設定 CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 初始化 Telethon 客戶端
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

# ==========================================
# 2. 伺服器啟動與關閉事件
# ==========================================
@app.on_event("startup")
async def startup_event():
    await client.connect()
    if not await client.is_user_authorized():
        print("❌ Telegram Session 無效，請重新取得 Session String！")
    else:
        print("✅ Telegram 客戶端連線成功！")

@app.on_event("shutdown")
async def shutdown_event():
    await client.disconnect()

# ==========================================
# 3. 根目錄健康檢查
# ==========================================
@app.get("/")
def read_root():
    return {"status": "ok", "message": "Meowtube API 正常運作中 🚀"}

# ==========================================
# 4. 核心功能：影片串流端點
# ==========================================
@app.get("/stream/{message_id}")
async def stream_video(message_id: int, request: Request, is_secret: bool = False):
    target_chat_id = SECRET_CHAT_ID if is_secret else PUBLIC_CHAT_ID
    
    try:
        # 第一次嘗試抓取
        message = await client.get_messages(target_chat_id, ids=int(message_id))
        
        # 🛡️ 防護機制：解決 Telethon 論壇模式群組快取問題
        if not message or not message.media:
            print("⚠️ 找不到訊息，嘗試強制刷新 Telethon 快取...")
            await client.get_dialogs()
            message = await client.get_messages(target_chat_id, ids=int(message_id))
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Telegram 讀取失敗: {str(e)}")
        
    if not message or not message.media:
         raise HTTPException(status_code=404, detail="找不到影片，或該訊息不含媒體檔案")
         
    file_size = message.file.size
    
    # 🛡️ 核心修復 1：強制宣告為影片格式
    mime_type = "video/mp4" 
    
    range_header = request.headers.get("Range")
    if range_header:
        start, end = range_header.replace("bytes=", "").split("-")
        start = int(start)
        end = int(end) if end else file_size - 1
    else:
        start = 0
        end = file_size - 1
        
    chunk_size = end - start + 1
    
    # 建立非同步的影片區塊生成器
    async def video_generator():
        try:
            bytes_left = chunk_size
            # 🛡️ 核心修復 2：直接傳入 message 物件，保留私密群組的下載權限上下文
            async for chunk in client.iter_download(message, offset=start):
                # 🛡️ 核心修復 3：將 Telethon 的 memoryview 強制轉型為標準 bytes
                chunk_bytes = bytes(chunk)
                
                if bytes_left <= 0:
                    break
                if len(chunk_bytes) >= bytes_left:
                    yield chunk_bytes[:bytes_left]
                    break
                yield chunk_bytes
                bytes_left -= len(chunk_bytes)
                
        except Exception as e:
            print(f"❌ 影片串流發生致命錯誤 (Message ID: {message_id}): {str(e)}")

    # 🛡️ 核心修復 4：嚴格遵守 HTTP 規範，精準設定 Headers
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Length": str(chunk_size),
        "Content-Type": mime_type,
    }
    
    if range_header:
        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
        status_code = 206
    else:
        status_code = 200
        
    return StreamingResponse(video_generator(), status_code=status_code, headers=headers)

# ==========================================
# 5. 除錯專用：Telegram 檔案探照燈
# ==========================================
@app.get("/debug/{message_id}")
async def debug_telegram_message(message_id: int, is_secret: bool = False):
    target_chat_id = SECRET_CHAT_ID if is_secret else PUBLIC_CHAT_ID
    
    try:
        message = await client.get_messages(target_chat_id, ids=message_id)
        
        if not message:
            await client.get_dialogs()
            message = await client.get_messages(target_chat_id, ids=message_id)
            
        if not message:
            return {"status": "failed", "detail": "Telegram 回報：完全找不到這則訊息"}
            
        return {
            "status": "success",
            "target_chat_id": target_chat_id,
            "message_id": message.id,
            "text": message.text,
            "has_media": bool(message.media),
            "media_type": type(message.media).__name__ if message.media else "無媒體",
            "file_name": message.file.name if (message.media and message.file) else "未知",
            "file_size_bytes": message.file.size if (message.media and message.file) else 0,
        }
    except Exception as e:
        return {"status": "error", "error_message": str(e)}
