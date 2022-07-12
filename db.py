import sqlalchemy as sa
from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy import Integer
from sqlalchemy.orm import declarative_base


Base = declarative_base()


class Towns(Base):
    __tablename__ = 'towns'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100))
    population = Column(Integer)
    link = Column(String(400))


async def insert_data(engine, df):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)  # данных не много, можно при каждом обновлении удалять таблицу
        await conn.run_sync(Base.metadata.create_all)  # и создавать/заполнять с нуля
        await conn.execute(sa.insert(Towns), df.to_dict(orient='records'))


async def search(engine, town, offset):
    async with engine.begin() as conn:
        select = sa.select(Towns).filter(Towns.name.ilike(f'%{town}%')).limit(50).offset(offset)  # поиск городов по названию
        result = await conn.execute(select)
    return result


async def search_town_by_id(engine, _id):
    async with engine.begin() as conn:
        town = await conn.execute(sa.select(Towns).where(Towns.id == int(_id)))

    return town.first()
