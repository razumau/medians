import asyncio
import logging
from collections import namedtuple
from datetime import datetime as dt
from itertools import chain
from typing import Dict, List, Tuple

import aiofiles
import aiohttp
import asyncpg
import logzero

from credentials import postgres_creds

logger = logzero.setup_logger(logfile='log.log',
                              level=logging.INFO, maxBytes=1_000_000)

CONNECTION_LIMIT = 100
TEAMS_LIMIT = 10000

TeamRelease = namedtuple('TeamRelease', 'id release_id release_date rating')


async def get_teams():
    logger.info('Getting list of teams')
    postgres_connection = await asyncpg.connect(**postgres_creds())
    result = await postgres_connection.fetch('''SELECT ratingId FROM teams''')
    await postgres_connection.close()
    ids = [r[0] for r in result]
    logger.info(f'{len(ids)} active teams')
    return ids


async def fetch_team_ratings(semaphore, team_id: int):
    url = f'http://rating.chgk.info/api/teams/{team_id}/rating.json'
    async with semaphore:
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as response:
                    if team_id % 100 == 0:
                        logger.info(team_id)
                    if response.status == 200:
                        team_ratings = await response.json()
                        return process_team(team_ratings)
                    else:
                        return []
            except aiohttp.client_exceptions.ClientError as e:
                logger.error(e)
                logger.error(url)
                return await fetch_team_ratings(semaphore, team_id)


def process_release(release: Dict) -> TeamRelease:
    return TeamRelease(int(release['idteam']),
                       int(release['idrelease']),
                       dt.strptime(release['date'], '%Y-%m-%d'),
                       int(release['rating']))


def process_team(team_ratings: List[Dict]) -> List[Tuple]:
    return sorted([process_release(r)
                   for r in team_ratings if r['formula'] == 'b'],
                  key=lambda tr: tr.release_date)


async def create_temp_table(postgres_connection, table: str):
    async with aiofiles.open(f'./sql/tables/{table}.sql') as query_file:
        ddl = await query_file.read()
    query = f'''DROP TABLE IF EXISTS {table}_temp;
                CREATE TABLE IF NOT EXISTS {table}_temp
                ({ddl});'''
    return await postgres_connection.execute(query)


async def import_data(postgres_connection, data: List, table: str):
    return await postgres_connection.copy_records_to_table(f'{table}_temp',
                                                           records=data)


async def replace_old_table(postgres_connection, table: str):
    table_short = table.split('.')[-1]
    query_replace = f'''DROP TABLE IF EXISTS {table}_old CASCADE;
                    CREATE TABLE IF NOT EXISTS {table} (id int);
                    ALTER TABLE {table} RENAME TO {table_short}_old;
                    ALTER TABLE {table}_temp RENAME TO {table_short};'''
    return await postgres_connection.execute(query_replace)


async def drop_old_table(postgres_connection, table):
    query_drop = f'DROP TABLE IF EXISTS {table}_old CASCADE;'
    return await postgres_connection.execute(query_drop)


async def recreate_indexes(postgres_connection):
    async with aiofiles.open(f'./sql/indexes.sql') as query_file:
        indexes = await query_file.read()
    return await postgres_connection.execute(indexes)


async def create_functions(postgres_connection):
    async with aiofiles.open(f'./sql/functions.sql') as query_file:
        functions = await query_file.read()
    try:
        return await postgres_connection.execute(functions)
    except asyncpg.exceptions.DuplicateFunctionError:
        pass


async def save_data(data):
    table = 'team_releases'
    postgres_connection = await asyncpg.connect(**postgres_creds())

    await create_temp_table(postgres_connection, table)
    logger.info(f'Created {table}_temp table')

    import_result = await import_data(postgres_connection, data, table)
    import_count = import_result.split(' ')[-1]
    logger.info(f'Copied {import_count} records to {table}_temp')

    await replace_old_table(postgres_connection, table)
    logger.info(f'Replaced {table} with the new one')

    await drop_old_table(postgres_connection, table)
    logger.info(f'Dropped {table}_old')

    await recreate_indexes(postgres_connection)
    logger.info(f'Recreated indexes')

    await create_functions(postgres_connection)
    logger.info(f'Created SQL functions')


def main():
    logger.info('Fetching ratings')
    loop = asyncio.get_event_loop()
    semaphore = asyncio.Semaphore(CONNECTION_LIMIT)

    data = loop.run_until_complete(get_teams())
    tasks = [fetch_team_ratings(semaphore, team_id)
             for team_id in data]

    data = loop.run_until_complete(
        asyncio.gather(*tasks, return_exceptions=False))
    data_flattened = chain.from_iterable(data)
    asyncio.get_event_loop().run_until_complete(save_data(data_flattened))


if __name__ == '__main__':
    main()
