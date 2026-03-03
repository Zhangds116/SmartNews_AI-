import logging
import hashlib
import redis
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from langchain_ollama import OllamaLLM
from config import OLLAMA_MODEL
from table import SessionLocal, News, Keyword, init_db,KeywordConfig
from fetch_news import fetch_news
from mail_utils import send_email
import warnings

warnings.filterwarnings("ignore")

# ================= 日志 =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


init_db()

# ================= Redis =================
redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

def is_duplicate(hash_value: str):
    return redis_client.exists(f"news:hash:{hash_value}")

def save_hash(hash_value: str):
    redis_client.setex(f"news:hash:{hash_value}", 3 * 24 * 3600, 1)


IMMEDIATE_KEYWORD_KEY = "news:immediate_keywords"
if not redis_client.exists(IMMEDIATE_KEYWORD_KEY):
    redis_client.sadd(IMMEDIATE_KEYWORD_KEY, "伊朗", "紧急事件", "财务公告")



def load_keywords_from_db():
    session = SessionLocal()
    try:
        keywords = session.query(Keyword).all()
        return [k.name for k in keywords]
    finally:
        session.close()


# ================= LLM =================
llm = OllamaLLM(
    model=OLLAMA_MODEL,
    temperature=0,
    num_predict=1500,
    timeout=60
)



def generate_hash(title: str, url: str):
    raw = (title + url).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def parse_structured_text(text: str):
    """
    解析 AI 输出，返回包含普通关键词和即时关键词匹配字段
    """
    data = {}
    if not text:
        return None
    text = text.replace("```", "").strip()
    for line in text.split("\n"):
        if ":" in line:
            key, value = line.split(":", 1)
            data[key.strip()] = value.strip()
    if "标题" not in data:
        return None

    def split_list(value):
        return [x.strip() for x in value.split(",") if x.strip()]

    data["领域"] = split_list(data.get("领域", ""))
    data["主要单位"] = split_list(data.get("主要单位", ""))
    data["匹配关键词"] = split_list(data.get("匹配关键词", ""))
    data["匹配即时关键词"] = split_list(data.get("匹配即时关键词", ""))
    return data



def analyze_news(news_item: dict):
    ordinary_keywords = load_keywords_from_db()
    immediate_keywords = list(redis_client.smembers(IMMEDIATE_KEYWORD_KEY))

    prompt = f"""
你是一个新闻信息抽取系统。

任务：
从新闻中提取以下字段：
- 标题
- 领域（最多3个，用逗号分隔）
- 地区
- 主要单位（最多3个，用逗号分隔）
- 来源
- 时间
- 内容总结（不超过200字）
- 匹配关键词（从普通专栏关键词列表中选择，严格匹配）
- 匹配即时关键词（从即时关键词列表中选择，严格匹配）

⚠ 输出规则：
1. 只输出字段结果
2. 不要解释
3. 不要重复本提示内容
4. 不要输出示例
5. 直接输出结果
6. 如果关键词不匹配，留空

字段输出格式必须为：

标题:
领域:
地区:
主要单位:
来源:
时间:
内容总结:
匹配关键词:
匹配即时关键词:

普通关键词列表：
{ordinary_keywords}

即时关键词列表：
{immediate_keywords}

=====================
下面是新闻内容：
标题：{news_item['title']}
来源：{news_item['source']}
时间：{news_item['publish_time']}
内容：{news_item['content']}
=====================

现在开始输出：
"""
    try:
        result = llm.invoke(prompt)
        parsed = parse_structured_text(result)
        if parsed:
            parsed["_original_url"] = news_item.get("url")
        return parsed
    except Exception as e:
        logger.warning(f"结构化异常: {e}")
        return None



def send_immediate_email(news_data: dict):
    """
    如果新闻匹配即时关键词，立即发送邮件
    且必须数据库配置 enable_immediate = True
    """
    matched = news_data.get("匹配即时关键词", [])
    if not matched:
        return

    session = SessionLocal()

    try:
        # 遍历每个匹配的即时关键词
        for kw_name in matched:

            keyword = session.query(Keyword).filter(
                Keyword.name == kw_name
            ).first()

            if not keyword:
                continue

            config = session.query(KeywordConfig).filter(
                KeywordConfig.keyword_id == keyword.id
            ).first()


            if not config or not config.enable_immediate:
                continue

            url_hash = generate_hash(
                news_data.get("标题", ""),
                news_data.get("_original_url", "")
            )


            if redis_client.exists(f"news:immediate:{url_hash}:{kw_name}"):
                continue

            redis_client.setex(
                f"news:immediate:{url_hash}:{kw_name}",
                3 * 24 * 3600,
                1
            )

            content = f"<h2>即时新闻通知</h2>"
            content += f"<p>{news_data.get('时间')} - {news_data.get('标题')}</p>"
            content += f"<p>来源: {news_data.get('来源')}</p>"
            content += f"<p>匹配即时关键词: {kw_name}</p>"
            content += f"<p><a href='{news_data.get('_original_url')}'>查看原文</a></p>"

            send_email(f"[即时] {kw_name}", content)

            logger.info(f"📨 已发送即时邮件: {kw_name}")

    except Exception as e:
        logger.error(f"发送即时邮件失败: {e}")
    finally:
        session.close()



def save_to_db(data: dict):
    session = SessionLocal()
    try:
        title = data.get("标题", "")
        url = data.get("_original_url", "")

        url_hash = generate_hash(title, url)
        if is_duplicate(url_hash):
            logger.info(f"⚠ 3日内重复: {title}")
            return

        try:
            published_at = datetime.strptime(data.get("时间", ""), "%Y-%m-%d %H:%M:%S")
        except:
            published_at = datetime.now()

        news = News(
            title=title,
            url=url,
            url_hash=url_hash,
            keywords=data.get("匹配关键词", []),
            domain=data.get("领域", [])[:3],
            region=data.get("地区", ""),
            main_entities=data.get("主要单位", [])[:3],
            source=data.get("来源", ""),
            published_at=published_at,
            summary=data.get("内容总结", "")[:500]
        )

        session.add(news)
        session.commit()
        save_hash(url_hash)

        logger.info(f"✓ 入库成功: {title} | 关注栏目: {data.get('匹配关键词') or '无'}")


        send_immediate_email(data)

    except Exception as e:
        session.rollback()
        logger.error(f"入库失败: {e}")
    finally:
        session.close()


def run_pipeline():
    logger.info("开始获取新闻...")
    news_list = fetch_news()
    total = len(news_list)
    logger.info(f"API返回 {total} 条新闻")

    batch_size = 8
    max_workers = 2

    for batch_start in range(0, total, batch_size):
        batch = news_list[batch_start: batch_start + batch_size]
        logger.info(f"处理批次 {batch_start//batch_size + 1} "
                    f"(第 {batch_start+1} - {min(batch_start+batch_size, total)} 条)")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(analyze_news, news): news for news in batch}
            for future in as_completed(futures):
                structured_data = future.result()
                if structured_data:
                    save_to_db(structured_data)

        logger.info("当前批次完成\n")

    logger.info("全部处理完成")



if __name__ == "__main__":
    while True:
        logger.info("===== 定时任务开始 =====")
        run_pipeline()
        logger.info("===== 等待 30 分钟 =====")
        time.sleep(1800)