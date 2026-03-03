# ===================== database.py =====================
from sqlalchemy import create_engine, Column, Integer, String, DateTime, JSON, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime

DATABASE_URL = ""

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False}
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


class News(Base):
    __tablename__ = "news"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(500))
    url = Column(String(500))
    url_hash = Column(String(64), unique=True, index=True)
    domain = Column(JSON)
    region = Column(String(100))
    keywords = Column(JSON)
    main_entities = Column(JSON)
    source = Column(String(200))
    published_at = Column(DateTime)
    summary = Column(String(200))
    created_at = Column(DateTime, default=datetime.utcnow)


class KeywordConfig(Base):
    __tablename__ = "keyword_config"

    id = Column(Integer, primary_key=True, index=True)
    keyword_id = Column(Integer, ForeignKey("keyword.id"), unique=True, nullable=False)


    enable_digest = Column(Boolean, default=True)

    digest_time = Column(String(5), default="22:30")

    enable_immediate = Column(Boolean, default=True)


    keyword = relationship("Keyword", back_populates="config")


class Keyword(Base):
    __tablename__ = "keyword"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, index=True)


    config = relationship("KeywordConfig", uselist=False, back_populates="keyword")



def init_db():
    """初始化数据库，创建所有表"""
    print("正在创建数据库表...")
    Base.metadata.create_all(bind=engine)
    print("数据库表创建成功！")


    from sqlalchemy import inspect
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    print(f"已创建的表: {tables}")



if __name__ == "__main__":
    print("正在删除旧表...")
    Base.metadata.drop_all(bind=engine)

    print("正在创建新表...")
    Base.metadata.create_all(bind=engine)


    from sqlalchemy import inspect

    inspector = inspect(engine)
    tables = inspector.get_table_names()
    print(f"数据库重建完成！当前表: {tables}")