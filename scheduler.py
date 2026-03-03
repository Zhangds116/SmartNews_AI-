from apscheduler.schedulers.background import BackgroundScheduler
from table import SessionLocal, News, Keyword,KeywordConfig
from datetime import datetime, timedelta
from mail_utils import send_email
from langchain_ollama import OllamaLLM
from config import OLLAMA_MODEL
import logging


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


llm = OllamaLLM(
    model=OLLAMA_MODEL,
    temperature=0,
    num_predict=1500,
    timeout=60
)

import re

def ai_summarize(news_list, keyword_name):
    """
    使用 AI 对该专栏新闻生成整体摘要，仅返回纯文本，不显示思考过程
    """
    if not news_list:
        return ""

    prompt = f"""
请你扮演一个新闻摘要系统，为“{keyword_name}”专栏生成一段中文摘要。
要求：
- 综合以下新闻标题和内容
- 概括主要事件、趋势、信息，不需要重复每条新闻
- 输出精炼，不超过 200 字
- 只返回摘要文本，不要任何额外标记、三引号或思考过程

新闻内容：
"""
    for news in news_list:
        prompt += f"标题: {news.title}\n内容: {news.summary}\n"

    try:

        summary = llm.invoke(prompt)


        summary = summary.replace("```", "").strip()


        summary = re.sub(r"<think>.*?</think>", "", summary, flags=re.DOTALL).strip()

        return f"<h3>AI 汇总:</h3><p>{summary}</p>"

    except Exception as e:
        logger.warning(f"AI汇总异常: {e}")
        return ""

def digest_job():
    session = SessionLocal()
    now = datetime.now()
    current_time_str = now.strftime("%H:%M")
    last_day = now - timedelta(days=1)

    keywords = session.query(Keyword).all()

    for kw in keywords:

        config = session.query(KeywordConfig).filter(
            KeywordConfig.keyword_id == kw.id
        ).first()


        if not config or not config.enable_digest:
            continue


        if config.digest_time != current_time_str:
            continue

        news_list = session.query(News).filter(
            News.keywords.contains([kw.name]),
            News.created_at > last_day
        ).all()

        if not news_list:
            continue

        content = f"<h2>{kw.name} - 最近新闻摘要</h2><ul>"
        for news in news_list:
            content += f"<li>{news.published_at.strftime('%Y-%m-%d %H:%M:%S')}: {news.title} - <a href='{news.url}'>原文</a></li>"
        content += "</ul>"

        content += ai_summarize(news_list, kw.name)

        send_email(f"[摘要] {kw.name}", content)

        logger.info(f"已发送摘要邮件: {kw.name}")

    session.close()


def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(digest_job, 'cron', minute="*")
    scheduler.start()



if __name__ == "__main__":
    digest_job()