import os
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from telethon import TelegramClient
from telethon.sessions import StringSession

app = FastAPI(title="家庭劇院串流 API")

# 設定 CORS，允許 Nuxt 前端跨網域請求
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # 之後若部署到 Vercel，可以把這裡改成你的 Vercel 網址以增加安全性
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 載入環境變數 (Render 後台設定)
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
TARGET_GROUP_ID = int(os.environ.get("TARGET_GROUP_ID", 0))

# 初始化 Telegram 用戶端
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

@app.on_event("startup")
async def startup_event():
    """伺服器啟動時，自動連接 Telegram"""
    await client.connect()

@app.get("/")
def read_root():
    """健康檢查端點 (用給 cron-job.org 喚醒伺服器用)"""
    return {"status": "Home Theater Streaming API is awake and running!"}

# ==========================================
# ⬆️ 1. 影片上傳端點
# ==========================================
@app.post("/upload/")
async def upload_movie(
    file: UploadFile = File(...), 
    topic_id: int = Form(None), # 若你的群組沒有開啟論壇模式(Topics)，這個可以留空
    caption: str = Form(None)
):
    if not client.is_connected():
        await client.connect()

    # Render 提供 /tmp 目錄供暫存，非常適合大檔案過渡
    temp_file_path = f"/tmp/{file.filename}"
    
    try:
        # 將前端傳來的影片分塊寫入 Render 的暫存空間
        with open(temp_file_path, "wb") as buffer:
            while chunk := await file.read(1024 * 1024): # 每次讀寫 1MB
                buffer.write(chunk)
        
        final_caption = caption if caption else f"🎬 電影上傳：{file.filename}"

        # 上傳至 Telegram
        message = await client.send_file(
            TARGET_GROUP_ID,
            file=temp_file_path,
            reply_to=topic_id,
            caption=final_caption,
            force_document=True,
            supports_streaming=True # 標記為支援串流
        )

        return {
            "success": True,
            "filename": file.filename,
            "message_id": message.id
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"上傳失敗: {str(e)}")
    finally:
        # 確保上傳後刪除暫存檔，釋放 Render 空間
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)

# ==========================================
# ⬇️ 2. 影片網頁串流播放端點 (核心功能)
# ==========================================
@app.get("/stream/{message_id}")
async def stream_movie(message_id: int):
    if not client.is_connected():
        await client.connect()

    # 取得影片檔案
    message = await client.get_messages(TARGET_GROUP_ID, ids=message_id)
    if not message or not message.media:
        raise HTTPException(status_code=404, detail="在 Telegram 中找不到此影片")

    # 建立一個非同步生成器，不斷從 Telegram 拉取影片流
    async def video_streamer():
        # 以 1MB 為單位拉取，完美適配 Render 512MB 記憶體限制
        async for chunk in client.iter_download(message.media, chunk_size=1024 * 1024):
            yield chunk

    # 關鍵：回傳 StreamingResponse，並指定 media_type，不使用 attachment 標頭
    # 這樣瀏覽器的 <video> 標籤才能直接邊載邊播
    return StreamingResponse(
        video_streamer(), 
        media_type="video/mp4" 
    )