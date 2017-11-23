create index teams_team_releases_idx on team_releases using hash (team_id);
create index release_date_team_releases_idx on team_releases using hash (release_date);