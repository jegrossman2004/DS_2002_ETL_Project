CREATE DATABASE mets_data;
USE mets_data;

#QUERY 2: On average, how many scoring plays were there per game when the Mets played any other MLB team?
SELECT 
    AVG(scoring_plays) AS avg_scoring_plays, s.opponent
FROM
    mets_data.mets_game_scoring_plays sp
        JOIN
    mets_data.mets_schedule s ON sp.game_number = s.game_number
GROUP BY s.opponent
ORDER BY avg_scoring_plays;

#QUERY 2: What were the average total runs scored and the average left, center, and right field wall distance of each team's stadium for every MLB team? Ordered by the average distance descending.
SELECT 
    st.team_name,
    ROUND(SUM(st.runs_per_game + st.runs_against_per_game),
            1) AS total_runs,
    ROUND(AVG(s.left_field_distance + s.center_field_distance + s.right_field_distance) / 3,
            2) AS avg_distance
FROM
    mlb_standings st
        JOIN
    team_data td ON td.team_name = st.team_name
        JOIN
    mlb_stadiums s ON s.team_abbreviation = td.team_abbreviation
GROUP BY st.team_name
ORDER BY avg_distance DESC;


#QUERY 3: In which games did MLB.com NOT upload 10 highlights from the game??
SELECT 
    COUNT(h.highlight_links) AS num_highlights,
    s.game_number,
    s.date
FROM
    mets_highlights h
        JOIN
    mets_schedule s ON h.game_number = s.game_number
GROUP BY s.game_number,s.date
HAVING NOT COUNT(h.highlight_links) = 10;
