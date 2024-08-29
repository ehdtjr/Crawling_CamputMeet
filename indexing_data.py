import pandas as pd
import numpy as np
from sqlalchemy import create_engine, Column, Integer, String, Float, JSON
from sqlalchemy.orm import declarative_base, sessionmaker
import json

# 데이터베이스 연결 정보
DB_USER = "postgres"
DB_PASSWORD = "qwe123"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "CampusMeet"

# SQLAlchemy 엔진 생성
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# 모델 정의
Base = declarative_base()

class FoodStore(Base):
    __tablename__ = 'food_stores'

    id = Column(Integer, primary_key=True)
    food_name = Column(String)
    address = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    operating_hours = Column(JSON)
    phone_number = Column(String)
    website = Column(String)

# 테이블 생성
Base.metadata.create_all(engine)

# 엑셀 파일 읽기
df = pd.read_excel('food_data_stores.xlsx', keep_default_na=False)

# 데이터 전처리 함수
def preprocess_data(value, column):
    if pd.isna(value) or value == '' or value == 'NULL':
        return None
    if column == 'operating_hours':
        if value.startswith('{') and value.endswith('}'):
            try:
                return json.loads(value.replace("'", '"'))
            except json.JSONDecodeError:
                return None
        return None
    return value

# 모든 열에 대해 전처리 적용
for column in df.columns:
    df[column] = df[column].apply(lambda x: preprocess_data(x, column))

# 세션 생성
Session = sessionmaker(bind=engine)
session = Session()

# 데이터 삽입
for _, row in df.iterrows():
    food_store = FoodStore(
        food_name=row['food_name'],
        address=row['address'],
        latitude=row['latitude'],
        longitude=row['longitude'],
        operating_hours=row['operating_hours'],
        phone_number=row['phone_number'],
        website=row['website']
    )
    session.add(food_store)

# 변경사항 커밋
try:
    session.commit()
    print("데이터가 성공적으로 PostgreSQL 데이터베이스에 저장되었습니다.")
except Exception as e:
    session.rollback()
    print(f"데이터 저장 중 오류가 발생했습니다: {e}")
finally:
    # 세션 종료
    session.close()

print("모든 처리가 완료되었습니다.")