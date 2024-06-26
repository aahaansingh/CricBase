import pandas as pd 
import numpy as np 
import os
import glob
import json

class DataRead :
    def __init__ (self, path:str) :
        """
        Creates a database object implimented as a group of pandas dataframes.
        
        Arguments:
            path (str): The path to the cricbase IPL JSON files.
        """
        self.path = path
        self.match = self.match_data()
        self.player, self.player_match = self.player_data()
        self.delivery, self.wickets, self.extras, self.fielder_wickets = self.delivery_data()
        self.player_scorecards()

    def match_key(self, data) :
        """
        Helper function that gets the match number and season of a given scorecard file.
        
        Arguments:
            data : the scorecard object, read from JSON.
        Returns:
            season : The IPL season in which the match occured.
            match_number : The IPL match number, or type of playoff game.
        """
        season = data["info"]["season"]
        match_number = None
        if "match_number" in data["info"]["event"] :
            match_number = str(data["info"]["event"]["match_number"])
        elif "stage" in data["info"]["event"] :
            match_number = data["info"]["event"]["stage"]
        return season, match_number

    def match_data(self) :
        """
        Creates a Dataframe containing general data on match location/outcomes.
        
        Returns: 
            match_df : The aforementioned Dataframe
        """
        match_data_array = []
        match_attrs = ["season", "match_number", "city", "start_date", "winner", "batting_first", "chasing", "eliminator"]
        for filename in glob.glob(os.path.join(self.path, '*.json')):
            data = json.load(open(filename))
            city = None
            start_date = None
            winner = None
            eliminator = None
            season, match_number = self.match_key(data)
            if "city" in data["info"] :
                city = data["info"]["city"]
            if "dates" in data["info"] :
                if len(data["info"]["dates"]) > 0 :
                    start_date = data["info"]["dates"][0]
            if "winner" in data["info"]["outcome"] :
                winner = data["info"]["outcome"]["winner"]
            elif "result" in data["info"]["outcome"] :
                winner = data["info"]["outcome"]["result"]
            if "eliminator" in data["info"]["outcome"] :
                eliminator = data["info"]["outcome"]["eliminator"]

            match_data_list = [season, match_number, city,
                    start_date, winner, data["info"]["teams"][0],
                    data["info"]["teams"][1], eliminator]
            match_data_array.append(match_data_list)
        match_df = pd.DataFrame(match_data_array, columns=match_attrs)
        return match_df

    def player_data(self) :
        """
        Returns a table matching players to their matches played.
        
        Returns :
            player_df : A table matching player name to various identifiers; the dataframe
            version of people.csv, which should be in the passed data directory.
            player_match_df : a relationship that maps players to games they played in."""
        player_df = pd.read_csv(os.path.join(self.path, 'people.csv'))
        player_match_array = []
        # full_player_match_attrs = ["name", "player_id", "season", "match_number", "team", "runs_scored", "fours", "sixes", 
        #                       "out", "balls_faced", "position", "wickets", "runs_conceded", "balls_delivered",
        #                       "fours_conceded", "sixes_conceded", "wides", "no_balls"]
        player_match_attrs = ["name", "player_id", "season", "match_number", "team"]
        for filename in glob.glob(os.path.join(self.path, '*.json')):
            data = json.load(open(filename))
            season, match_number = self.match_key(data)
            for player in data["info"]["registry"]["people"] :
                team = None
                for team_name in data["info"]["players"] :
                    if player in data["info"]["players"][team_name] :
                        team = team_name
                if not team == None : # is an official
                    player_match = [player, data["info"]["registry"]["people"][player], season, match_number,
                                team]
                    player_match_array.append(player_match)
        player_match_df = pd.DataFrame(player_match_array, columns=player_match_attrs)
        return player_df, player_match_df

    def delivery_data(self) :
        """
        Creates tables with info on every single IPL delivery.
        This function might be vectorized at a later date to better handle more data

        Returns:
            delivery_df : basic information on every delivery.
            wicket_df : extra information on wicket deliveries.
            extra_df : extra information on extra (byes, wides, nbs, etc.) deliveries.
            fielder_wicket_df : maps wickets to fielders involved in the dismissal.
        """
        delivery_array = []
        delivery_features = ["season", "match_number", "team_batting", "over", "number", "batter",
                            "bowler", "non_striker", "extras", "runs", "total_runs", "wickets", "match_id"]
        wicket_array = []
        wicket_features = ["season", "match_number", "team_batting", "over", "number", "player_out", "type"]
        extra_array = []
        extra_features = ["season", "match_number", "team_batting", "over", "number", "byes", "legbyes", "noballs", "penalty", "wides"]
        fielder_wicket_array = []
        fielder_wicket_features = ["season", "match_number", "team_batting", "over", "number", "id"] # not name, which isn't necessarily unique
        for filename in glob.glob(os.path.join(self.path, '*.json')):
            data = json.load(open(filename))
            season, match_number = self.match_key(data)
            registry = data["info"]["registry"]["people"]
            for innings_data in data["innings"] :
                team_batting = innings_data["team"]
                for over_data in innings_data["overs"] :
                    over = over_data["over"]
                    for number, delivery_data in enumerate(over_data["deliveries"]) :
                        batter_name = delivery_data["batter"]
                        batter_id = registry[batter_name]
                        bowler_name = delivery_data["bowler"]
                        bowler_id = registry[bowler_name]
                        non_striker_name = delivery_data["non_striker"]
                        non_striker_id = registry[non_striker_name]
                        runs = delivery_data["runs"]["batter"]
                        total_runs = delivery_data["runs"]["total"]
                        wickets = 0
                        extras = delivery_data["runs"]["extras"]

                        if "wickets" in delivery_data :
                            wickets = len(delivery_data["wickets"])
                            for wicket_data in delivery_data["wickets"] :
                                wicket_type = wicket_data["kind"]
                                player_out_name = wicket_data["player_out"]
                                player_out_id = registry[player_out_name]
                                wicket_list = [season, match_number, team_batting, over, number, player_out_id, wicket_type]
                                wicket_array.append(wicket_list)
                                if "fielders" in wicket_data:
                                    for fielder_data in wicket_data["fielders"] :
                                        fielder_name = fielder_data["name"]
                                        fielder_id = registry[fielder_name]
                                        fielder_wicket_list = [season, match_number, team_batting, over, number, fielder_id]
                                        fielder_wicket_array.append(fielder_wicket_list)
                        
                        if "extras" in delivery_data :
                            extras_data = delivery_data["extras"]
                            byes, legbyes, noballs, penalty, wides = 0, 0, 0, 0, 0
                            if "byes" in extras_data :
                                byes = extras_data["byes"]
                            if "legbyes" in extras_data :
                                legbyes = extras_data["legbyes"]
                            if "noballs" in extras_data :
                                noballs = extras_data["noballs"]
                            if "penalty" in extras_data :
                                penalty = extras_data["penalty"]
                            if "wides" in extras_data :
                                wides = extras_data["wides"]
                            extra_list = [season, match_number, team_batting, over, number, byes, 
                                        legbyes, noballs, penalty, wides]
                            extra_array.append(extra_list)
                        # In this databases, overs + deliveries are zero indexed
                        delivery_list = [season, match_number, team_batting, over, number, batter_id, 
                                        bowler_id, non_striker_id, extras, runs, total_runs, wickets, str(season) + " " + str(match_number)]
                        delivery_array.append(delivery_list)
        delivery_df = pd.DataFrame(delivery_array, columns=delivery_features)
        wicket_df = pd.DataFrame(wicket_array, columns=wicket_features)
        extra_df = pd.DataFrame(extra_array, columns=extra_features)
        fielder_wicket_df = pd.DataFrame(fielder_wicket_array, columns=fielder_wicket_features)
        return delivery_df, wicket_df, extra_df, fielder_wicket_df

    def player_scorecards(self) :
        """
        Extracts scorecard data for each player in each match, inserting it into the database
        object's player_match_df.
        """
        full_extra_df = self.delivery.merge(self.extras, on=["season", "match_number", "team_batting", "over", "number"])
        full_wickets_df = self.delivery.merge(self.wickets, on=["season", "match_number", "team_batting", "over", "number"])
        get_runs_scored(self)
        get_runs_conceded(self, full_extra_df)
        get_fours_sixes_data(self)
        get_balls_faced(self, full_extra_df)
        get_extras_delivered(self, full_extra_df)
        get_num_wickets(self, full_wickets_df)
        get_batsman_out(self, full_wickets_df)
        get_batting_position(self)

def get_runs_scored(self) :
    """Gets the runs scored by each batsman in each match, adding this to player_match."""
    # The runs scored per ball corresponds directly to the runs field of each delivery
    runs_scored = self.delivery[["season", "match_number", "batter", "runs"]]\
        .groupby(by=["season", "match_number", "batter"], sort=False, as_index=False).sum()
    self.player_match = self.player_match.merge(runs_scored, how="left", 
                                                left_on=["season", "match_number", "player_id"], 
                                                right_on=["season", "match_number", "batter"])
    self.player_match = self.player_match.rename(columns={"runs":"runs_scored"})

def get_runs_conceded(self, full_extra_df) :
    """
    Gets the runs conceded by each bowler in each match, adding this to player_match.
    
    Arguments: 
        full_extra_df: extra_df inner joined with delivery_df
    """
    runs_conceded = self.delivery[["season", "match_number", "bowler", "runs"]]\
        .groupby(by=["season", "match_number", "bowler"], sort=False, as_index=False).sum()
    all_extras_conceded = full_extra_df[["season", "match_number", "bowler", "byes", "legbyes", "noballs", "penalty", "wides"]]\
        .groupby(by=["season", "match_number", "bowler"], sort=False, as_index=False).sum()
    all_wides_noballs_conceded = all_extras_conceded[["season", "match_number", "bowler", "wides", "noballs"]]
    all_runs_conceded = runs_conceded.merge(all_wides_noballs_conceded, 
                                            on=["season", "match_number", "bowler"], 
                                            how="left")
    all_runs_conceded["runs_conceded"] = all_runs_conceded[["runs", "wides", "noballs"]].sum(axis=1)# sum on the hor axis
    self.player_match = self.player_match.merge(all_runs_conceded[["season", "match_number", "bowler", "runs_conceded", "wides", "noballs"]], 
                                            how="left", left_on=["season", "match_number", "player_id"], 
                                            right_on=["season", "match_number", "bowler"])
    self.player_match = self.player_match.drop(["bowler", "batter"], axis=1)
    
def get_fours_sixes_data(self) :
    """Gets the fours and sixes scored/conceded by each player in each match."""
    # Unfortunately CricSheet data doesn't distinguish between boundaries and run fours/sixes
    all_fours = self.delivery.loc[np.where(self.delivery["runs"] == 4)]
    all_sixes = self.delivery.loc[np.where(self.delivery["runs"] == 6)]

    fours_scored = all_fours[["season", "match_number", "batter", "runs"]]\
        .groupby(by=["season", "match_number", "batter"], sort=False, as_index=False).count()
    fours_scored = fours_scored.rename(columns={"runs":"fours_scored"})
    sixes_scored = all_sixes[["season", "match_number", "batter", "runs"]]\
        .groupby(by=["season", "match_number", "batter"], sort=False, as_index=False).count()
    sixes_scored = sixes_scored.rename(columns={"runs":"sixes_scored"})
    fours_conceded = all_fours[["season", "match_number", "bowler", "runs"]]\
        .groupby(by=["season", "match_number", "bowler"], sort=False, as_index=False).count()
    fours_conceded = fours_conceded.rename(columns={"runs":"fours_conceded"})
    sixes_conceded = all_sixes[["season", "match_number", "bowler", "runs"]]\
        .groupby(by=["season", "match_number", "bowler"], sort=False, as_index=False).count()
    sixes_conceded = sixes_conceded.rename(columns={"runs":"sixes_conceded"})

    self.player_match = self.player_match.merge(fours_scored, 
                                            how="left", left_on=["season", "match_number", "player_id"], 
                                            right_on=["season", "match_number", "batter"])
    self.player_match = self.player_match.merge(fours_conceded, 
                                            how="left", left_on=["season", "match_number", "player_id"], 
                                            right_on=["season", "match_number", "bowler"])
    self.player_match = self.player_match.drop(["bowler", "batter"], axis=1)

    self.player_match = self.player_match.merge(sixes_scored, 
                                            how="left", left_on=["season", "match_number", "player_id"], 
                                            right_on=["season", "match_number", "batter"])
    self.player_match = self.player_match.merge(sixes_conceded, 
                                            how="left", left_on=["season", "match_number", "player_id"], 
                                            right_on=["season", "match_number", "bowler"])
    self.player_match = self.player_match.drop(["bowler", "batter"], axis=1)

def get_balls_faced(self, full_extra_df) :
    """
    Gets the runs faced by each batsman in each match.
    
    Arguments: 
        full_extra_df: extra_df inner joined with delivery_df
    """
    total_balls_faced = self.delivery[["season", "match_number", "batter", "runs"]]\
        .groupby(by=["season", "match_number", "batter"], sort=False, as_index=False).count()
    total_balls_faced = total_balls_faced.rename(columns={"runs":"balls_faced"})
    wides_faced = full_extra_df[["season", "match_number", "batter", "wides"]].loc[np.where(full_extra_df["wides"] > 0)]
    noballs_faced = full_extra_df[["season", "match_number", "batter", "noballs"]].loc[np.where(full_extra_df["noballs"] > 0)]
    
    # Get counts for wides and noballs grouped by batter
    total_wides_faced = wides_faced.groupby(by=["season", "match_number", "batter"], sort=False, as_index=False).count()
    total_noballs_faced = noballs_faced.groupby(by=["season", "match_number", "batter"], sort=False, as_index=False).count()
    total_balls_faced = total_balls_faced.merge(total_wides_faced, how="left", on=["season", "match_number", "batter"])
    total_balls_faced = total_balls_faced.merge(total_noballs_faced, how="left", on=["season", "match_number", "batter"])
    total_balls_faced = total_balls_faced.fillna(0)
    total_balls_faced["balls_faced"] -= total_balls_faced["wides"] + total_balls_faced["noballs"]
    total_balls_faced = total_balls_faced.drop(["wides", "noballs"], axis=1)
    self.player_match = self.player_match.merge(total_balls_faced, 
                                            how="left", left_on=["season", "match_number", "player_id"], 
                                            right_on=["season", "match_number", "batter"])
    self.player_match = self.player_match.drop(["batter"], axis=1)


def get_num_wickets(self, full_wickets_df) :
    """
    Gets the number of wickets taken by each bowler in each match.
    
    Arguments: 
        full_wickets_df: wicket_df inner joined with delivery_df
    """
    bowler_wickets = full_wickets_df.loc[np.where(full_wickets_df["type"] != "run out")]
    wickets_taken = bowler_wickets[["season", "match_number", "bowler", "wickets"]]\
        .groupby(by=["season", "match_number", "bowler"], sort=False, as_index=False).count()
    self.player_match = self.player_match.merge(wickets_taken, 
                                            how="left", left_on=["season", "match_number", "player_id"], 
                                            right_on=["season", "match_number", "bowler"])
    self.player_match = self.player_match.drop(["bowler"], axis=1)

def get_batsman_out(self, full_wickets_df) :
    """
    Checks whether batsman was out in their innings.
    
    Arguments: 
        full_wickets_df: wicket_df inner joined with delivery_df
    """
    batsman_out = full_wickets_df[["season", "match_number", "player_out", "wickets"]]\
        .groupby(by=["season", "match_number", "player_out"], sort=False, as_index=False).count()
    batsman_out = batsman_out.rename(columns={"wickets":"out"})
    batsman_out["out"] = 1
    self.player_match = self.player_match.merge(batsman_out, 
                                            how="left", left_on=["season", "match_number", "player_id"], 
                                            right_on=["season", "match_number", "player_out"])
    self.player_match = self.player_match.drop(["player_out"], axis=1)
    self.player_match = self.player_match.fillna(0)

def get_batting_position(self) :
    """Gets the batting position for each player in the match"""
    delivery_df_chopped = self.delivery[["season", "match_number", "team_batting", "over", "number", "batter"]]
    # need to account for non-striker b/c they could be run out first ball + other edge cases
    delivery_df_chopped_ns = self.delivery[["season", "match_number", "team_batting", "over", "number", "non_striker"]]
    delivery_df_chopped_ns = delivery_df_chopped_ns.rename(columns={"non_striker":"batter"})
    delivery_df_chopped = pd.concat([delivery_df_chopped, delivery_df_chopped_ns], axis=0)
    # sort_index puts the deliveries in order for each match, then cumcount() gets the position
    # of each batter
    player_position = delivery_df_chopped.sort_index()[["season", "match_number", "team_batting", "batter"]].drop_duplicates()
    player_position["position"] = player_position.groupby(by=["season", "match_number", "team_batting"]).cumcount()
    player_position["position"] += 1 # not zero-indexed as a standard, unlike delivery
    player_position = player_position.drop(["team_batting"], axis=1)
    self.player_match = self.player_match.merge(player_position, 
                                            how="left", left_on=["season", "match_number", "player_id"], 
                                            right_on=["season", "match_number", "batter"])
    self.player_match = self.player_match.drop(["batter"], axis=1)

def get_extras_delivered(self, full_extra_df) :
    """
    Gets the number of wides and noballs delivered by each bowler in each match
    
    Arguments: 
        full_extra_df: extra_df inner joined with delivery_df
    """
    total_balls_delivered = self.delivery[["season", "match_number", "bowler", "runs"]].groupby(by=["season", "match_number", "bowler"], 
                                                                                                sort=False, as_index=False).count()
    total_balls_delivered = total_balls_delivered.rename(columns={"runs":"balls_delivered"})
    wides_delivered = full_extra_df[["season", "match_number", "bowler", "wides"]].loc[np.where(full_extra_df["wides"] > 0)]
    noballs_delivered = full_extra_df[["season", "match_number", "bowler", "noballs"]].loc[np.where(full_extra_df["noballs"] > 0)]
    total_wides_delivered = wides_delivered.groupby(by=["season", "match_number", "bowler"], sort=False, as_index=False).count()
    total_noballs_delivered = noballs_delivered.groupby(by=["season", "match_number", "bowler"], sort=False, as_index=False).count()
    
    total_balls_delivered = total_balls_delivered.merge(total_wides_delivered, 
                                                        how="left", on=["season", "match_number", "bowler"])
    total_balls_delivered = total_balls_delivered.merge(total_noballs_delivered, 
                                                        how="left", on=["season", "match_number", "bowler"])
    
    total_balls_delivered = total_balls_delivered.fillna(0)
    total_balls_delivered["balls_delivered"] -= total_balls_delivered["wides"] + total_balls_delivered["noballs"]
    total_balls_delivered = total_balls_delivered.drop(["wides", "noballs"], axis=1)
    self.player_match = self.player_match.merge(total_balls_delivered, 
                                            how="left", left_on=["season", "match_number", "player_id"], 
                                            right_on=["season", "match_number", "bowler"])
    self.player_match = self.player_match.drop(["bowler"], axis=1)