import os
import uuid
import shutil
import asyncio
import threading
import time
import glob
from datetime import datetime
import psycopg2
import psycopg2.extras

import uvicorn
from fastapi import FastAPI, UploadFile, File, Form, Request, Query
from fastapi.responses import JSONResponse

# 导入项目中现有的模块
from AzurStats.database.base import AzurStatsDatabase
from AzurStats.config.config import CONFIG
from module.config.utils import deep_get
from module.logger import logger

app = FastAPI(title="Azur Stats Server")

# 获取配置中的图片保存目录
IMAGE_FOLDER = str(deep_get(CONFIG, 'Folder.images'))
DB_CONFIG = deep_get(CONFIG, 'Database')

# 确保图片目录存在
os.makedirs(IMAGE_FOLDER, exist_ok=True)

def get_db_connection():
    # 注意：根据项目原逻辑，源数据库在 azurstat，因此连接时需切换或在SQL中显式指定
    return psycopg2.connect(**DB_CONFIG)

def get_dict_db_connection():
    # 返回使用 DictCursor 的连接，方便 API 返回 JSON
    config = DB_CONFIG.copy()
    config['cursor_factory'] = psycopg2.extras.RealDictCursor
    return psycopg2.connect(**config)

def init_queue_db():
    """初始化用于接收图片的队列数据库和表"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS azurstat;")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS azurstat.img_images (
                    id SERIAL PRIMARY KEY,
                    imgid CHAR(16) NOT NULL UNIQUE,
                    path VARCHAR(255) NOT NULL,
                    date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    device_id VARCHAR(255) DEFAULT '',
                    genre VARCHAR(255) DEFAULT '',
                    combat_count INT DEFAULT 0
                );
            """)
        conn.commit()
        logger.info("队列数据库 azurstat 及其表 img_images 初始化完成")
    except Exception as e:
        logger.error(f"初始化队列数据库失败: {e}")
    finally:
        conn.close()

@app.post("/api/upload")
async def upload_image(
    request: Request,
    file: UploadFile = File(...),
    genre: str = Form(...),
    device_id: str = Form(...),
    combat_count: int = Form(...)
):
    """
    接收 ALAS 客户端上传的截图
    """
    imgid = uuid.uuid4().hex[:16]
    
    # 构建相对路径和绝对路径 (按年月分子目录是个好习惯，这里为简便直接放根目录或按天)
    date_str = datetime.now().strftime("%Y/%m/%d")
    relative_dir = f"/imgs/{date_str}"
    absolute_dir = os.path.join(IMAGE_FOLDER, date_str)
    os.makedirs(absolute_dir, exist_ok=True)
    
    relative_path = f"{relative_dir}/{imgid}.png"
    absolute_path = os.path.join(absolute_dir, f"{imgid}.png")
    
    # 1. 保存文件到本地磁盘
    try:
        with open(absolute_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        logger.error(f"保存文件失败: {e}")
        return JSONResponse(status_code=500, content={"error": "Failed to save file"})

    # 2. 记录到 azurstat.img_images
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "INSERT INTO azurstat.img_images (imgid, path, device_id, genre, combat_count) VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(sql, (imgid, relative_path, device_id, genre, combat_count))
        conn.commit()
        logger.info(f"收到新图片上传: {imgid} ({genre})")
    except Exception as e:
        logger.error(f"写入数据库失败: {e}")
        return JSONResponse(status_code=500, content={"error": "Database error"})
    finally:
        conn.close()

    return {"status": "success", "imgid": imgid}

@app.get("/api/data/{table_name}")
async def get_data(
    table_name: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    device_id: str = None,
    genre: str = None,
    server: str = None
):
    """
    根据表名从 azurstat_data 中获取数据
    """
    # 限制允许查询的表，防止SQL注入
    allowed_tables = [
        "research_projects", "research_items", 
        "meowfficer_talents", "commission_items", "battle_items", "opsi_items"
    ]
    if table_name not in allowed_tables:
        return JSONResponse(status_code=400, content={"error": f"Invalid table name. Allowed tables: {allowed_tables}"})
    
    conn = get_dict_db_connection()
    try:
        with conn.cursor() as cursor:
            query = f"SELECT * FROM azurstat_data.{table_name} WHERE 1=1"
            params = []
            
            if device_id:
                query += " AND device_id = %s"
                params.append(device_id)
            if genre:
                query += " AND genre = %s"
                params.append(genre)
            if server:
                query += " AND server = %s"
                params.append(server)
                
            query += " LIMIT %s OFFSET %s"
            params.extend([limit, offset])
            
            cursor.execute(query, tuple(params))
            results = cursor.fetchall()
            
            # Count total records
            count_query = f"SELECT COUNT(*) as total FROM azurstat_data.{table_name} WHERE 1=1"
            count_params = []
            if device_id:
                count_query += " AND device_id = %s"
                count_params.append(device_id)
            if genre:
                count_query += " AND genre = %s"
                count_params.append(genre)
            if server:
                count_query += " AND server = %s"
                count_params.append(server)
            cursor.execute(count_query, tuple(count_params))
            total = cursor.fetchone()['total']
            
        return {"status": "success", "data": results, "total": total, "limit": limit, "offset": offset}
    except Exception as e:
        logger.error(f"Failed to query data: {e}")
        return JSONResponse(status_code=500, content={"error": "Database query failed"})
    finally:
        conn.close()

def background_analysis_task():
    """
    后台守护线程：不断轮询数据库进行解析，并在解析后清理文件
    """
    logger.info("后台分析与清理任务已启动...")
    db = AzurStatsDatabase()
    
    while True:
        try:
            total = db.get_total_updates()
            if total > 0:
                logger.info(f"检测到 {total} 张新截图，开始分析流程...")
                # 调用项目原生的 update 批量处理所有未解析的图片
                # db.update() 内部现在已经包含了清理文件的逻辑
                db.update()
                
        except Exception as e:
            logger.error(f"后台分析任务发生异常: {e}")
            os._exit(1)
            
        # 休眠 10 秒后再次检查（可根据服务器性能和实时性要求调整）
        time.sleep(10)

def cleanup_lock_files():
    """
    启动时清理项目目录下所有的 .lock 文件
    """
    project_dir = os.path.dirname(os.path.abspath(__file__))
    lock_files = glob.glob(os.path.join(project_dir, "**", "*.lock"), recursive=True)
    for lock_file in lock_files:
        try:
            os.remove(lock_file)
            logger.info(f"Deleted lock file: {lock_file}")
        except Exception as e:
            logger.error(f"Failed to delete lock file {lock_file}: {e}")

@app.on_event("startup")
async def startup_event():
    cleanup_lock_files()
    # 初始化队列数据库
    init_queue_db()
    # 启动后台守护线程进行分析
    thread = threading.Thread(target=background_analysis_task, daemon=True)
    thread.start()

if __name__ == "__main__":
    logger.info("启动 Azur Stats 集成服务端...")
    # 绑定 0.0.0.0 使其能被外网/其他容器访问，端口可按需修改
    uvicorn.run(app, host="0.0.0.0", port=22240)
