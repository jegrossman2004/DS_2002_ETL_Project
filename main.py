import statsapi
import pandas as pd
import re
import pymysql

# TABLE 1: Scoring Plays In Mets Games
mets_game_scoring_plays = pd.DataFrame()
mets_game_scoring_plays_list = []

mets_game_dates = []
mets_game_nos = []
games_162 = list(range(1, 163))
try:
    for x in [y for y in statsapi.schedule(team=121, start_date='04/07/2022', end_date='10/06/2022')]:
        if x['status'] == 'Final':
            mets_game_dates.append(x['game_date'])
            mets_game_nos.append(x['game_id'])
    mets_game_scoring_plays['game_number'] = games_162
    for x in mets_game_nos:
        count_scoring = statsapi.game_scoring_plays(x).count("Mets:")
        mets_game_scoring_plays_list.append(count_scoring)
    mets_game_scoring_plays['scoring_plays'] = mets_game_scoring_plays_list
except:
    print("There was an error retrieving the Mets' scoring play data")
# USED SOURCE 1: API
# ---------------------------------------------------------------
# TABLE 2: Mets 2022 Schedule
try:
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
except:
    print("There was an error retrieving the Mets' schedule data")
# USED SOURCE 2: Baseball reference
# --------------------------------------------------------
# TABLE 3: MLB Stadiums
try:
    mlb_stadiums = pd.read_csv('ballparks.csv')
    mlb_stadiums.loc[1, 'team_name'] = "ARI"
    mlb_stadiums.loc[11, 'team_name'] = "KCR"
    mlb_stadiums.loc[22, 'team_name'] = "SDP"
    mlb_stadiums.loc[24, 'team_name'] = "SFG"
    mlb_stadiums.loc[26, 'team_name'] = "TBR"
    mlb_stadiums = mlb_stadiums.rename(
        columns={"team_name": "team_abbreviation", "left_field": "left_field_distance",
                 "center_field": "center_field_distance", "right_field": "right_field_distance",
                 "roof": "roof_percentage"})
except:
    print("There was an error rerieving the MLB stadium data")
# USED SOURCE 3: Kaggle Database
# ------------------------------------------------------

# TABLE 4: MLB Standings Data
try:
    mlb_standings = pd.read_csv('mlbstandings.csv')
    mlb_standings = mlb_standings.rename(
        columns={"Rk": "rank", "Tm": "team_name", "W": "win_total", "L": "loss_total", "W-L%": "win_loss_percent",
                 "R": "runs_per_game", "RA": "runs_against_per_game", "Rdiff": "run_differential_per_game",
                 "SOS": "strength_of_schedule", "SRS": "simple_rating_system", "pythWL": "pyth_record",
                 "Luck": "luck"})
except:
    print("There was an error rerieving the MLB standings data")
# USED SOURCE 2: Baseball Reference
# --------------------------------------------------------------------------------
# TABLE 5: Mets Highlights Data (From Source 1, API)
try:
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
except:
    print("There was an error retrieving the Mets' highlight data")
# USED SOURCE 1: API
# --------------------------------
# TABLE 6: TEAM ABBREVATIONS
try:
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
except:
    print("There was an error rerieving the team name/abbreviations data")
# -----------------------------------------------------------------
host_ip = '127.0.0.1'
port = '3306'
user_id = 'root'
pwd = 'TommyCutlets15!?'
db_name = 'mets_data'
try:
    conn = pymysql.connect(host=host_ip, user=user_id, password=pwd, database=db_name)
    # QUERY 1: On average, how many scoring plays were there per game when the Mets played any other MLB team?
    df = pd.read_sql('SELECT td.team_name, AVG(scoring_plays) AS avg_scoring_plays FROM '
                     'mets_data.mets_game_scoring_plays sp JOIN mets_data.mets_schedule s ON sp.game_number = '
                     's.game_number JOIN team_data td ON s.opponent = td.team_abbreviation GROUP BY td.team_name ORDER '
                     'BY avg_scoring_plays;', conn)
    print(df)
    print("----------------------------------------")
    # QUERY 2: What were the average total runs scored and the average of the left, center, and right field wall distance of each team's stadium for every MLB team? Ordered by the average distance descending.
    df = pd.read_sql('SELECT st.team_name, ROUND(SUM(st.runs_per_game + st.runs_against_per_game), 1) AS total_runs, '
                     'ROUND(AVG(s.left_field_distance + s.center_field_distance + s.right_field_distance) / 3, '
                     '2) AS avg_distance FROM mlb_standings st JOIN team_data td ON td.team_name = st.team_name JOIN '
                     'mlb_stadiums s ON s.team_abbreviation = td.team_abbreviation GROUP BY st.team_name ORDER BY '
                     'avg_distance DESC;', conn)
    print(df)
    print("----------------------------------------")
    # QUERY 3: In which games did MLB.com NOT upload 10 highlights from the game??
    df = pd.read_sql('SELECT s.game_number, s.date,COUNT(h.highlight_links) AS num_highlights FROM mets_highlights h '
                     'JOIN mets_schedule s ON h.game_number = s.game_number GROUP BY s.game_number,s.date HAVING NOT '
                     'COUNT(h.highlight_links) = 10;', conn)
    print(df)

except:
    print("Error: unable to fetch data")

conn.close()
