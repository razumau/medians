from typing import Dict, List

import asyncpg

from credentials import postgres_creds


async def get_team_name(team) -> str:
    postgres_connection = await asyncpg.connect(**postgres_creds())
    query = '''
        SELECT name 
        FROM teams 
        WHERE ratingid = $1
    '''
    data = await postgres_connection.fetch(query, team)
    return data[0]


async def get_all_releases_for_team(team) -> List[Dict]:
    postgres_connection = await asyncpg.connect(**postgres_creds())
    query = '''
        SELECT release_date, release_id, 
            three_months, twelve_months 
        FROM medians 
        WHERE team_id = $1
        ORDER BY release_date DESC
    '''
    data = await postgres_connection.fetch(query, team)
    return [dict(row) for row in data]


async def get_all_teams_for_release(release_id=None):
    postgres_connection = await asyncpg.connect(**postgres_creds())
    query = f'''
        SELECT team_id, team_name, 
            three_months, twelve_months 
        FROM medians 
        WHERE release_id = $1
        ORDER BY three_months DESC
    '''
    data = await postgres_connection.fetch(query, release_id)
    return [dict(row) for row in data]
