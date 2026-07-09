import os
import re
import uuid
from fastapi import FastAPI, HTTPException, Request, Response, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from telethon import TelegramClient
from telethon.sessions import StringSession

app = FastAPI(title="Meowtube HF API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
TARGET_GROUP_ID = int(os.environ.get("TARGET_GROUP_ID", 0))

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

# 💡 儲存背景任務狀態的字典
upload_tasks = {}

@app.get("/")
def read_root():
    return {"status": "Meowtube API is running on Hugging Face!"}

# ==========================================
# ⬆️ 背景上傳至 Telegram 核心函數
# ==========================================
async def process_upload_to_tg(task_id: str, temp_file_path: str, topic_id: int, caption: str):
    try:
        if not client.is_connected():
            await client.connect()

        # 上傳到 TG，大檔案可能會花費數分鐘
        message = await client.send_file(
            TARGET_GROUP_ID,
            file=temp_file_path,
            reply_to=topic_id,
            caption=caption,
            supports_streaming=True
        )
        # 標記為成功並存入 message_id
        upload_tasks[task_id]["status"] = "completed"
        upload_tasks[task_id]["message_id"] = message.id
    except Exception as e:
        upload_tasks[task_id]["status"] = "failed"
        upload_tasks[task_id]["error"] = str(e)
    finally:
        # 確保刪除 HF 上的暫存檔案，釋放空間
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)

# ==========================================
# ⬆️ 1. 影片/MP3 上傳端點 (改為不卡頓的背景接收)
# ==========================================
@app.post("/upload/")
async def upload_movie(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...), 
    topic_id: int = Form(None), 
    caption: str = Form(None)
):
    task_id = str(uuid.uuid4())
    upload_tasks[task_id] = {"status": "processing", "message_id": None, "error": None}

    # 安全地將大檔案寫入 HF 的 /tmp 暫存區
    safe_filename = file.filename.replace(" ", "_")
    temp_file_path = f"/tmp/{task_id}_{safe_filename}"
    
    try:
        with open(temp_file_path, "wb") as buffer:
            while chunk := await file.read(1024 * 1024):
                buffer.write(chunk)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"主機接收檔案失敗: {e}")

    # 💡 將耗時的 TG 轉發動作丟到背景執行，立刻回傳 task_id 給網頁
    background_tasks.add_task(process_upload_to_tg, task_id, temp_file_path, topic_id, caption)

    return {
        "success": True,
        "task_id": task_id
    }

# ==========================================
# ⬆️ 1.5 狀態查詢端點 (網頁會來這裡輪詢進度)
# ==========================================
@app.get("/upload_status/{task_id}")
async def check_upload_status(task_id: str):
    if task_id not in upload_tasks:
        raise HTTPException(status_code=404, detail="找不到該任務")
    return upload_tasks[task_id]

# ==========================================
# ⬇️ 2. 完美串流端點 (206 斷點續傳)
# ==========================================
@app.get("/stream/{message_id}")
async def stream_movie(request: Request, message_id: int):
    if not client.is_connected():
        await client.connect()

    message = await client.get_messages(TARGET_GROUP_ID, ids=message_id)
    if not message or not getattr(message, 'file', None):
        raise HTTPException(status_code=404, detail="找不到此影片檔")

    file_size = message.file.size
    range_header = request.headers.get("Range")
    start = 0
    end = file_size - 1
    status_code = 200

    if range_header:
        range_match = re.match(r"bytes=(\d+)-(\d*)", range_header)
        if range_match:
            start = int(range_match.group(1))
            end_str = range_match.group(2)
            if end_str: end = int(end_str)
            status_code = 206 

    if start >= file_size:
        return Response(status_code=416, headers={"Content-Range": f"bytes */{file_size}"})

    req_length = end - start + 1
    CHUNK_SIZE = 1024 * 1024
    aligned_start = start - (start % CHUNK_SIZE)
    skip_bytes = start - aligned_start 

    async def video_streamer():
        yielded_bytes = 0
        skip = skip_bytes
        async for chunk in client.iter_download(
            message.media, offset=aligned_start, request_size=CHUNK_SIZE
        ):
            chunk_data = bytes(chunk)
            if skip > 0:
                chunk_data = chunk_data[skip:]
                skip = 0
            if yielded_bytes + len(chunk_data) > req_length:
                chunk_data = chunk_data[:req_length - yielded_bytes]
                yield chunk_data
                break
            yield chunk_data
            yielded_bytes += len(chunk_data)

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Length": str(req_length),
        "Content-Type": message.file.mime_type or "video/mp4",
    }
    if status_code == 206:
        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

    return StreamingResponse(video_streamer(), status_code=status_code, headers=headers, media_type=message.file.mime_type or "video/mp4")

# ==========================================
# 🗑️ 3. 刪除端點
# ==========================================
@app.delete("/delete/{message_id}")
async def delete_movie(message_id: int):
    if not client.is_connected():
        await client.connect()
    try:
        await client.delete_messages(TARGET_GROUP_ID, [message_id])
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
