from fastapi import FastAPI, Depends, Query, Request, Form
from sqlalchemy.orm import Session
from typing import Optional
import uvicorn
import math

from table import SessionLocal, News, Keyword, Base,KeywordConfig

from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine
from config import DATABASE_URL
from scheduler import start_scheduler, digest_job

app = FastAPI(title="AI News Tracking API")
templates = Jinja2Templates(directory="templates")


engine = create_engine(DATABASE_URL)

@app.on_event("startup")
def startup_event():
    Base.metadata.create_all(bind=engine)
    start_scheduler()



def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()



@app.get("/", response_class=HTMLResponse)
def home(
    request: Request,
    page: int = Query(1, ge=1),
    keyword: Optional[str] = None,
    db: Session = Depends(get_db)
):
    page_size = 10
    query = db.query(News)

    if keyword:
        query = query.filter(
            News.keywords.contains([keyword])
        )

    total = query.count()
    total_pages = math.ceil(total / page_size) if total > 0 else 1

    news_list = (
        query.order_by(News.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    keywords = db.query(Keyword).order_by(Keyword.id.desc()).all()


    for kw in keywords:
        if not kw.config:
            config = KeywordConfig(keyword_id=kw.id)
            db.add(config)
    db.commit()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "news_list": news_list,
            "keywords": keywords,
            "keyword": keyword,
            "page": page,
            "total_pages": total_pages
        }
    )



@app.post("/keyword/config/{keyword_id}")
def update_keyword_config(
    keyword_id: int,
    enable_digest: Optional[str] = Form(None),
    digest_time: str = Form(...),
    enable_immediate: Optional[str] = Form(None),
    db: Session = Depends(get_db)
):
    config = db.query(KeywordConfig).filter(
        KeywordConfig.keyword_id == keyword_id
    ).first()

    if not config:
        config = KeywordConfig(keyword_id=keyword_id)
        db.add(config)

    config.enable_digest = True if enable_digest == "on" else False
    config.enable_immediate = True if enable_immediate == "on" else False
    config.digest_time = digest_time

    db.commit()

    return RedirectResponse("/", status_code=303)



@app.get("/send_digest_now")
def send_digest_now():
    try:
        digest_job()
        return {"status": "success", "message": "摘要邮件已发送"}
    except Exception as e:
        return {"status": "error", "message": str(e)}



@app.post("/keyword/add")
def add_keyword(name: str = Form(...), db: Session = Depends(get_db)):
    name = name.strip()

    if not name:
        return RedirectResponse("/", status_code=303)

    if db.query(Keyword).filter(Keyword.name == name).first():
        return RedirectResponse("/", status_code=303)

    new_keyword = Keyword(name=name)
    db.add(new_keyword)
    db.commit()

    return RedirectResponse("/", status_code=303)



@app.get("/keyword/delete/{keyword_id}")
def delete_keyword(keyword_id: int, db: Session = Depends(get_db)):
    keyword = db.query(Keyword).filter(Keyword.id == keyword_id).first()
    if keyword:
        db.delete(keyword)
        db.commit()

    return RedirectResponse("/", status_code=303)


if __name__ == "__main__":
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)