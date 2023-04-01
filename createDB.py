import json
import os
import pandas as pd
import numpy as np
import csv


def load_pappalardo_match_data():
    folder = 'data/pappalardo/match_data/'
    data = []
    for filename in os.listdir(folder):
        if filename.endswith('.json'):
            with open(os.path.join(folder, filename), 'r') as f:
                data.extend(json.load(f))
    return data


def filter_match_data(data, row_mask):
    # Filter the generator expression to keep only the tuples where the second element is True
    filtered_data = list(filter(lambda x: x[1], zip(data, row_mask)))

    # Extract the first element from each tuple and convert the filtered generator expression to a list
    data_subset = [x[0] for x in filtered_data]
    return data_subset


def extract_match_data_by_year(data, year):
    years = [int(row['dateutc'][:4]) for row in data]
    year_mask = [y == year for y in years]

    return filter_match_data(data, year_mask)


def load_fifa_player_data_df(columns_of_interest=None):
    folder = 'data/fifa_data/'
    filename = 'detailed_fifa_2018_Dec_2017.csv'

    fifa_player_data = pd.read_csv(os.path.join(folder, filename))

    if columns_of_interest is not None:
        fifa_player_data = fifa_player_data[columns_of_interest]

    return fifa_player_data


def load_pappalardo_player_data():
    folder = 'data/pappalardo/player_data/'
    filename = 'players.json'

    # Open the JSON file
    with open(os.path.join(folder, filename), 'r') as f:
        # Load the JSON data
        data = json.load(f)

    return data


def create_pid_lookup_table():
    pid_to_fifa_pid = {}
    fifa_pid_to_pid = {}
    folder = 'data'
    filename = 'relational_db.csv'

    with open(os.path.join(folder, filename), newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            pid = int(row['pid'])
            fifa_pid = int(row['fifa_pid'])

            pid_to_fifa_pid[pid] = fifa_pid

            # Exclude the invalid PID
            if fifa_pid != -1:
                fifa_pid_to_pid[fifa_pid] = pid

    return pid_to_fifa_pid, fifa_pid_to_pid


def get_match_players(match_data):
    for team_name in match_data['teamsData']:
        side = match_data['teamsData'][team_name]['side']

        if side == 'home':
            local_team = match_data['teamsData'][team_name]['teamId']
            local_team_pids = [player['playerId'] for player in
                               match_data['teamsData'][team_name]['formation']['lineup']]
        else:
            visit_team = match_data['teamsData'][team_name]['teamId']
            visit_team_pids = [player['playerId'] for player in
                               match_data['teamsData'][team_name]['formation']['lineup']]

    return local_team, local_team_pids, visit_team, visit_team_pids


def find_players_position(pids, player_data):
    positions = [next((player['role']['code2'] for player in player_data if player['wyId'] == pid), None) for pid in pids]

    players_by_position = {
        'GK': [],
        'DF': [],
        'MD': [],
        'FW': []
    }

    for pid, pos in zip(pids, positions):
        if pos:
            players_by_position[pos].append(pid)

    return players_by_position



def get_player_fifa_overall_rating(player_id, pid_to_fifa_pid_lut, fifa_player_data_df):
    fifa_id = pid_to_fifa_pid_lut[player_id]

    ovr_rating = get_overall_rating(fifa_id, fifa_player_data_df)

    return ovr_rating


def get_overall_rating(sofifa_id, df):
    # Select rows with matching sofifa_id and extract overall rating
    try:
        overall_rating = df.loc[df['sofifa_id'] == sofifa_id, 'overall'].iloc[0]
    except IndexError:
        overall_rating = float('nan')

    return overall_rating


def get_avg_fifa_overall_rating(pids, pid_to_fifa_pid_lut, fifa_player_data_df):
    player_ratings = np.zeros(len(pids))

    for i, current_player_id in enumerate(pids):
        player_ratings[i] = get_player_fifa_overall_rating(current_player_id, pid_to_fifa_pid_lut, fifa_player_data_df)

    avg_rating = np.nanmean(player_ratings)

    return avg_rating, player_ratings


def get_fifa_stats(pids, player_data, fifa_player_data_df, pid_to_fifa_pid_lut):
    pids_by_position = find_players_position(pids, player_data)

    # GK rating
    avg_gk_rating, gk_ratings = get_avg_fifa_overall_rating(pids_by_position["GK"], pid_to_fifa_pid_lut,
                                                            fifa_player_data_df)

    # Avg. DF rating
    avg_def_rating, def_ratings = get_avg_fifa_overall_rating(pids_by_position["DF"], pid_to_fifa_pid_lut,
                                                              fifa_player_data_df)

    # Avg. MD rating
    avg_med_rating, med_ratings = get_avg_fifa_overall_rating(pids_by_position["MD"], pid_to_fifa_pid_lut,
                                                              fifa_player_data_df)

    # Avf. FW rating
    avg_fw_rating, fw_ratings = get_avg_fifa_overall_rating(pids_by_position["FW"], pid_to_fifa_pid_lut,
                                                            fifa_player_data_df)

    fifa_stats = {"AVG_GK": avg_gk_rating, "NUM_GK": len(gk_ratings), "AVG_DF": avg_def_rating,
                  "NUM_DF": len(def_ratings),
                  "AVG_MD": avg_med_rating, "NUM_MD": len(med_ratings), "AVG_FW": avg_fw_rating,
                  "NUM_FW": len(fw_ratings)}

    return fifa_stats


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    FIFA_WC_TOURNAMENT_ID = 28
    EURO_TOURNAMENT_ID = 102

    # Load Pappalardo (pp) match data set
    match_data_pp = load_pappalardo_match_data()

    # Get match data for 2018
    match_data_pp = extract_match_data_by_year(match_data_pp, 2018)

    # Remove international competition data
    mask = [(row['competitionId'] != FIFA_WC_TOURNAMENT_ID) and (row['competitionId'] != EURO_TOURNAMENT_ID) for row in
            match_data_pp]
    match_data_pp = filter_match_data(match_data_pp, mask)

    # Load EA FIFA data set
    fifa_player_data_df = load_fifa_player_data_df(['sofifa_id', 'short_name', 'age', 'overall', 'potential'])

    # Load Pappalardo (pp) player data set
    player_data_pp = load_pappalardo_player_data()

    # Load relational data base
    pid_to_fifa_pid, fifa_pid_to_pid = create_pid_lookup_table()

    local_team_gk = []
    local_team_df = []
    local_team_num_df = []
    local_team_md = []
    local_team_num_md = []
    local_team_fw = []
    local_team_num_fw = []

    visit_team_gk = []
    visit_team_df = []
    visit_team_num_df = []
    visit_team_md = []
    visit_team_num_md = []
    visit_team_fw = []
    visit_team_num_fw = []

    winner_vec = []

    all_players = []

    for match in match_data_pp:
        local_team, local_team_pids, visit_team, visit_team_pids = get_match_players(match)

        local_team_fifa_stats = get_fifa_stats(local_team_pids, player_data_pp, fifa_player_data_df, pid_to_fifa_pid)
        visit_team_fifa_stats = get_fifa_stats(visit_team_pids, player_data_pp, fifa_player_data_df, pid_to_fifa_pid)

        local_team_gk.append(local_team_fifa_stats['AVG_GK'])
        local_team_df.append(local_team_fifa_stats['AVG_DF'])
        local_team_num_df.append(local_team_fifa_stats['NUM_DF'])
        local_team_md.append(local_team_fifa_stats['AVG_MD'])
        local_team_num_md.append(local_team_fifa_stats['NUM_MD'])
        local_team_fw.append(local_team_fifa_stats['AVG_FW'])
        local_team_num_fw.append(local_team_fifa_stats['NUM_FW'])

        visit_team_gk.append(visit_team_fifa_stats['AVG_GK'])
        visit_team_df.append(visit_team_fifa_stats['AVG_DF'])
        visit_team_num_df.append(visit_team_fifa_stats['NUM_DF'])
        visit_team_md.append(visit_team_fifa_stats['AVG_MD'])
        visit_team_num_md.append(visit_team_fifa_stats['NUM_MD'])
        visit_team_fw.append(visit_team_fifa_stats['AVG_FW'])
        visit_team_num_fw.append(visit_team_fifa_stats['NUM_FW'])

        winner_id = match['winner']

        if winner_id == local_team:
            winner = "LOCAL"
        elif winner_id == visit_team:
            winner = "VISIT"
        elif winner_id == 0:
            winner = "DRAW"
        else:
            winner = "?"

        winner_vec.append(winner)

    # Create a dataframe to study the contrast patterns
    dataset = pd.DataFrame({
        'L_AVG_OVR_GK': local_team_gk,
        'L_AVG_OVR_DF': local_team_df,
        'L_NUM_DF': local_team_num_df,
        'L_AVG_OVR_MD': local_team_md,
        'L_NUM_MD': local_team_num_md,
        'L_AVG_OVR_FW': local_team_fw,
        'L_NUM_FW': local_team_num_fw,
        'V_AVG_OVR_GK': visit_team_gk,
        'V_AVG_OVR_DF': visit_team_df,
        'V_NUM_DF': visit_team_num_df,
        'V_AVG_OVR_MD': visit_team_md,
        'V_NUM_MD': visit_team_num_md,
        'V_AVG_OVR_FW': visit_team_fw,
        'V_NUM_FW': visit_team_num_fw,
        'WINNER': winner_vec
    })

    # Write the dataframe to a csv file
    filename = 'data/custom_database.csv'
    dataset.to_csv(filename, index=False)