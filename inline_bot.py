import os

from aiogram import Bot, Dispatcher, types, executor
import aiogram.utils.markdown as fmt

from dotenv import load_dotenv
import logging
from logging import FileHandler, Formatter

from sqlalchemy.ext.asyncio import create_async_engine

from db import insert_data, search
from parsers import parse_towns

load_dotenv()

API_TOKEN = os.getenv('BOT_TOKEN')
url = 'https://ru.wikipedia.org/wiki/%D0%93%D0%BE%D1%80%D0%BE%D0%B4%D1%81%D0%BA%D0%B8%D0%B5_%D0%BD%D0%B0%D1%81%D0%B5%D0%BB%D1%91%D0%BD%D0%BD%D1%8B%D0%B5_%D0%BF%D1%83%D0%BD%D0%BA%D1%82%D1%8B_%D0%9C%D0%BE%D1%81%D0%BA%D0%BE%D0%B2%D1%81%D0%BA%D0%BE%D0%B9_%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D0%B8'

POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_SERVER = os.getenv('POSTGRES_SERVER')
engine = create_async_engine(
    f'postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}/{POSTGRES_DB}',
    echo=True,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = FileHandler('bot.log', encoding='utf-8')
handler.setFormatter(Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s'))
logger.addHandler(handler)

bot = Bot(token=API_TOKEN, parse_mode=types.ParseMode.HTML)
dp = Dispatcher(bot)


@dp.message_handler(commands=['start', 'help'])
async def send_welcome(message: types.Message):
    await message.reply(fmt.text(
        f'Бот позволяет искать города и пгт Московской области.\n'
        f'Команда /update запускает парсинг страницы\n'
        f'{fmt.hcode("https://ru.wikipedia.org/wiki/Городские_населённые_пункты_Московской_области")}\n'
        f'и сохранение в базу данных\n'
        f'Для поиска в inline-режиме нужно обрабиться к боту\n'
        f'{fmt.hcode("@test_task_alef_bot")} название города или его часть\n'
        f'При выборе конкретного города, выводится его численность и ссылка на вики.'
    ))


@dp.message_handler(commands='update')  #
async def echo(message: types.Message):
    await message.answer(fmt.text('Идет обновление базы данных...'))
    logger.info('Updating DB')
    try:
        df = parse_towns(url)
        await insert_data(engine, df)
        await message.answer(fmt.text('База обновлена.'))
        logger.info('DB updated')
    except:
        logger.warning("Can't update database")
        await message.answer(fmt.text('Проблемы с соединением. Попробуйте позже.'))


@dp.inline_handler()
async def start_search(query: types.InlineQuery):
    offset = int(query.offset) if query.offset else 1
    towns = await search(engine, query.query, offset - 1)
    logger.info(f'Search: "{query.query}"')
    if towns:
        articles = []
        for town in towns:
            card = fmt.text(  # формируем карточку города:
                f'{fmt.hbold(town.name)}\n'
                f'Население: {town.population} чел.'
                f'{fmt.hide_link(town.link)}'
            )

            articles.append(
                types.InlineQueryResultArticle(
                    id=town.id,
                    title=town.name,
                    input_message_content=types.InputMessageContent(message_text=card, parse_mode="HTML")
                )
            )

        if len(articles) < 50:  # телеграм ограничивает инлайн сообщения не более 50 записей
            await query.answer(articles, cache_time=30, next_offset="", is_personal=True,  # если записей меньше 50
                               switch_pm_text='Перейти к боту', switch_pm_parameter="update")  # выдаем без офсета
        else:  # если больше 50 записей, выдаются 50, при прокрутке подгружается следующая порция
            await query.answer(articles, cache_time=30, next_offset=str(offset + 50), is_personal=True,
                               switch_pm_text='Перейти к боту', switch_pm_parameter="update")

    else:
        await query.answer([], cache_time=60)  # если ничего не найдено, выдается пустой список


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
