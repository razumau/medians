from sanic import Sanic
from sanic.exceptions import abort
from sanic.response import json

from db import (get_all_releases_for_team,
                get_all_teams_for_release,
                get_team_name)

app = Sanic()


@app.route("/team")
async def one_team_all_releases(request):
    args = request.raw_args
    team = args.get('id')
    if not team:
        abort(400, 'Team ID is not set.')
    try:
        team_id = int(team)
    except ValueError:
        abort(400, 'User ID should be an integer.')
    name = await get_team_name(team)
    releases = await get_all_releases_for_team(team)
    return json({'team_id': team_id, 'name': name, 'releases': releases})


@app.route("/release")
async def one_release_all_teams(request):
    args = request.raw_args
    release = args.get('id')
    if not release:
        abort(400, 'Release ID is not set.')
    try:
        release_id = int(release)
    except ValueError:
        abort(400, 'User ID should be an integer.')
    teams = await get_all_teams_for_release(release_id)
    return json({'team': release_id, 'releases': teams})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
