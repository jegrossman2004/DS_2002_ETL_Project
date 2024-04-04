import pip
import requests
import statsapi
import csv
import pandas as pd
import re
from sqlalchemy import create_engine

# TABLE 1: Scoring Plays In Mets Games
mets_game_scoring_plays = pd.DataFrame()
mets_game_scoring_plays_list = []
mets_game_dates = []
mets_game_nos = []
games_162 = list(range(1, 163))

for x in [y for y in statsapi.schedule(team=121, start_date='04/07/2022', end_date='10/06/2022')]:
    if x['status'] == 'Final':
        mets_game_dates.append(x['game_date'])
        mets_game_nos.append(x['game_id'])
mets_game_scoring_plays['game_number'] = games_162

for x in mets_game_nos:
    count_scoring = statsapi.game_scoring_plays(x).count("Mets:")
    mets_game_scoring_plays_list.append(count_scoring)

# print(mets_game_highlights)
mets_game_scoring_plays['scoring_plays'] = mets_game_scoring_plays_list
# SOURCE 1: API

# ---------------------------------------------------------------
# TABLE 2: Mets 2022 Schedule
pd.set_option('display.max_columns', None)
away_game_holder = []
mets_schedule = pd.read_csv('nym2022sched.csv')
mets_schedule = mets_schedule.drop(
    columns=[mets_schedule.columns[2], "Tm", "Inn", "W-L",
             "Rank", "GB",
             "Time", "D/N", "cLI", "Streak",
             "Orig. Scheduled"])
mets_schedule = mets_schedule.rename(
    columns={'Gm#': 'game_number', 'Date': 'date', 'Unnamed: 4': "away_game", "R": "mets_runs", "RA": "opp_runs",
             "Win": "winning_pitcher", "Loss": "losing_pitcher", "W/L": "mets_result", "Opp": "opponent",
             "Save": "save", "Attendance": "attendance"})
for x in mets_schedule["away_game"]:
    if x == "@":
        away_game_holder.append("Yes")
    else:
        away_game_holder.append("No")
mets_schedule["away_game"] = away_game_holder
# SOURCE 2: Baseball reference

# --------------------------------------------------------
# TABLE 3: MLB Stadiums
mlb_stadiums = pd.read_csv('ballparks.csv')
mlb_stadiums.loc[1, 'team_name'] = "ARI"
mlb_stadiums.loc[11, 'team_name'] = "KCR"
mlb_stadiums.loc[22, 'team_name'] = "SDP"
mlb_stadiums.loc[24, 'team_name'] = "SFG"
mlb_stadiums.loc[26, 'team_name'] = "TBR"
mlb_stadiums = mlb_stadiums.rename(
    columns={"team_name": "team_abbreviation", "left_field": "left_field_distance",
             "center_field": "center_field_distance", "right_field": "right_field_distance", "roof": "roof_percentage"})

# SOURCE 3: Kaggle Database
# ------------------------------------------------------
# TABLE 4: MLB Standings Data, SOURCE 2 Baseballreference
mlb_standings = pd.read_csv('mlbstandings.csv')
mlb_standings = mlb_standings.rename(
    columns={"Rk": "rank", "Tm": "team_name", "W": "win_total", "L": "loss_total", "W-L%": "win_loss_percent",
             "R": "runs_per_game", "RA": "runs_against_per_game", "Rdiff": "run_differential_per_game",
             "SOS": "strength_of_schedule", "SRS": "simple_rating_system", "pythWL": "pyth_record",
             "Luck": "luck"})
#MENTION YOU ADJUSTED THE COLUMNS BEFOREHAND BY CUTTING OFF EXCESS
# --------------------------------------------------------------------------------
# TABLE 5: Mets Highlights Data (From Source 1, API)
mets_highlights = pd.DataFrame()
highlight_titles = []
highlight_descriptions = []
highlight_links = []
game_no_highlights = []
game_no = 0
num_highlights = 0
for x in mets_game_nos:
    game_no += 1
    highlights = statsapi.game_highlights(x)
    highlights = highlights.replace("\n", "::: ")
    highlights = re.split(":::\s*", highlights)
    highlights = [x for x in highlights if x != '']
    while len(highlights) > 0:
        num_highlights += 1
        highlight_titles.append(highlights[0])
        highlight_descriptions.append(highlights[1])
        highlight_links.append(highlights[2])
        del highlights[0]
        del highlights[0]
        del highlights[0]
        game_no_highlights.append(game_no)

mets_highlights['highlight_titles'] = highlight_titles
mets_highlights['highlight_descriptions'] = highlight_descriptions
mets_highlights['highlight_links'] = highlight_links
mets_highlights['game_number'] = game_no_highlights
# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_colwidth', None)
#Mention very proud of this one
# --------------------------------
# TABLE 6: TEAM ABBREVATIONS
team_data = pd.DataFrame()
team_names = []
team_abbreviations = []
for x in mlb_standings['team_name']:
    team_names.append(x)
for x in mlb_stadiums['team_abbreviation']:
    team_abbreviations.append(x)
team_names = sorted(team_names)
team_abbreviations = sorted(team_abbreviations)
team_abbreviations[5] = "CWS"
team_abbreviations[6] = "CIN"
team_abbreviations[7] = "CLE"
team_abbreviations[8] = "COL"
team_abbreviations[23] = "SFG"
team_abbreviations[24] = "SEA"
team_data['team_name'] = team_names
team_data['team_abbreviation'] = team_abbreviations
"""print(mets_game_scoring_plays)
print(mets_schedule)
print(mlb_stadiums)
print(mlb_standings)
print(mets_highlights)
print(team_data)"""

dataframes = {
    'mets_game_scoring_plays': mets_game_scoring_plays,
    'mets_schedule': mets_schedule,
    'mlb_stadiums': mlb_stadiums,
    'mlb_standings': mlb_standings,
    'mets_highlights': mets_highlights,
    'team_data': team_data
}
db_username = 'root'
db_password = 'TommyCutlets15!?'
db_host = '127.0.0.1'
db_port = '3306'
db_name = 'mets_data'
db_connection_str = f'mysql+mysqlconnector://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
engine = create_engine(db_connection_str)
for table_name, df in dataframes.items():
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

print("DataFrames converted to SQL tables successfully.")
#ERROR MESSAGES
#Explain how you modified the incoming data
#mention you couldn't find a publicly available sql database for MLB
#submit all data used
#submit all SQL and Python Code
#describe ur process and clean up code