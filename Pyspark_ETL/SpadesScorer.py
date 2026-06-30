import streamlit as st
import pandas as pd

st.set_page_config(page_title="Spades Score Tracker", page_icon="♠", layout="wide")

BAG_LIMIT = 5
BAG_PENALTY = 50
ROUNDS = 13

st.title("♠ Spades Score Tracker")


def init_state():
    if "game_started" not in st.session_state:
        st.session_state.game_started = False
        st.session_state.players = []
        st.session_state.rounds = []
        st.session_state.scores = {}
        st.session_state.total_bags = {}
        st.session_state.penalties_applied = {}


def calculate_round_score(round_no, bid, tricks):
    if round_no == 13 and bid == 0:
        return 100 if tricks == 0 else -100

    if tricks >= bid:
        return bid * 10

    return bid * -10


def calculate_bags(bid, tricks):
    return max(tricks - bid, 0)


def recalculate_totals():
    st.session_state.scores = {p: 0 for p in st.session_state.players}
    st.session_state.total_bags = {p: 0 for p in st.session_state.players}
    st.session_state.penalties_applied = {p: 0 for p in st.session_state.players}

    for round_data in st.session_state.rounds:
        for entry in round_data:
            player = entry["Player"]

            st.session_state.scores[player] += entry["Round Score"]
            st.session_state.total_bags[player] += entry["Bags This Round"]

            expected_penalties = st.session_state.total_bags[player] // BAG_LIMIT
            new_penalties = expected_penalties - st.session_state.penalties_applied[player]

            if new_penalties > 0:
                st.session_state.scores[player] -= new_penalties * BAG_PENALTY
                st.session_state.penalties_applied[player] += new_penalties

            entry["Total Score After Round"] = st.session_state.scores[player]
            entry["Total Bags After Round"] = st.session_state.total_bags[player]
            entry["Bag Penalties Applied"] = st.session_state.penalties_applied[player]


init_state()

if not st.session_state.game_started:
    st.subheader("Game Setup")

    num_players = st.number_input(
        "Number of players",
        min_value=2,
        max_value=10,
        value=5,
        step=1
    )

    players = []

    for i in range(num_players):
        name = st.text_input(f"Player {i + 1} name", value=f"Player {i + 1}")
        players.append(name.strip())

    if st.button("Start Game"):
        if len(set(players)) != len(players):
            st.error("Player names must be unique.")
        elif any(name == "" for name in players):
            st.error("Player names cannot be empty.")
        else:
            st.session_state.players = players
            st.session_state.rounds = []
            st.session_state.scores = {p: 0 for p in players}
            st.session_state.total_bags = {p: 0 for p in players}
            st.session_state.penalties_applied = {p: 0 for p in players}
            st.session_state.game_started = True
            st.rerun()

else:
    current_round = len(st.session_state.rounds) + 1

    if current_round <= ROUNDS:
        st.subheader(f"Enter Round {current_round} Scores")
        st.caption(f"Total tricks won by all players must equal {current_round}.")

        round_entries = []
        total_tricks_preview = 0

        cols = st.columns(len(st.session_state.players))

        for idx, player in enumerate(st.session_state.players):
            with cols[idx]:
                st.markdown(f"### {player}")

                bid = st.number_input(
                    "Bid",
                    min_value=0,
                    max_value=current_round,
                    value=0,
                    step=1,
                    key=f"bid_{current_round}_{player}"
                )

                tricks = st.number_input(
                    "Tricks Won",
                    min_value=0,
                    max_value=current_round,
                    value=0,
                    step=1,
                    key=f"tricks_{current_round}_{player}"
                )

                round_score = calculate_round_score(current_round, bid, tricks)
                bags_this_round = calculate_bags(bid, tricks)

                total_tricks_preview += tricks

                st.write(f"Round Score: **{round_score}**")
                st.write(f"Bags: **{bags_this_round}**")

                round_entries.append({
                    "Round": current_round,
                    "Player": player,
                    "Bid": bid,
                    "Tricks Won": tricks,
                    "Round Score": round_score,
                    "Bags This Round": bags_this_round,
                    "Bag Penalty This Round": 0,
                    "Total Score After Round": 0,
                    "Total Bags After Round": 0,
                    "Bag Penalties Applied": 0
                })

        if total_tricks_preview == current_round:
            st.success(f"Total tricks entered: {total_tricks_preview}/{current_round}")
        else:
            st.warning(f"Total tricks entered: {total_tricks_preview}/{current_round}")

        if st.button("Save Round"):
            total_tricks = sum(entry["Tricks Won"] for entry in round_entries)

            if total_tricks != current_round:
                st.error(
                    f"Invalid entry. In Round {current_round}, total tricks won by all players "
                    f"must be exactly {current_round}. You entered {total_tricks}."
                )
            else:
                saved_round = []

                for entry in round_entries:
                    player = entry["Player"]

                    st.session_state.scores[player] += entry["Round Score"]
                    st.session_state.total_bags[player] += entry["Bags This Round"]

                    expected_penalties = st.session_state.total_bags[player] // BAG_LIMIT
                    new_penalties = expected_penalties - st.session_state.penalties_applied[player]

                    if new_penalties > 0:
                        penalty_points = new_penalties * BAG_PENALTY
                        st.session_state.scores[player] -= penalty_points
                        st.session_state.penalties_applied[player] += new_penalties
                        entry["Bag Penalty This Round"] = -penalty_points

                    entry["Total Score After Round"] = st.session_state.scores[player]
                    entry["Total Bags After Round"] = st.session_state.total_bags[player]
                    entry["Bag Penalties Applied"] = st.session_state.penalties_applied[player]

                    saved_round.append(entry)

                st.session_state.rounds.append(saved_round)
                st.rerun()

    else:
        st.success("Game Completed!")
        winner = max(st.session_state.scores, key=st.session_state.scores.get)
        st.header(f"🏆 {winner} Wins!")

    st.divider()

    st.subheader("Current Leaderboard")

    leaderboard = pd.DataFrame([
        {
            "Player": player,
            "Total Score": st.session_state.scores[player],
            "Total Bags": st.session_state.total_bags[player],
            "Bag Penalties Applied": st.session_state.penalties_applied[player],
        }
        for player in st.session_state.players
    ])

    leaderboard = leaderboard.sort_values("Total Score", ascending=False)
    st.dataframe(leaderboard, use_container_width=True, hide_index=True)

    st.divider()

    st.subheader("Round-wise Score History")

    if st.session_state.rounds:
        player_rows = []

        for player in st.session_state.players:
            row = {"Player": player}

            for round_data in st.session_state.rounds:
                round_no = round_data[0]["Round"]

                player_entry = next(
                    entry for entry in round_data if entry["Player"] == player
                )

                row[f"R{round_no} Score"] = player_entry["Round Score"]
                row[f"R{round_no} Bags"] = player_entry["Bags This Round"]

            player_rows.append(row)

        history_wide_df = pd.DataFrame(player_rows)
        st.dataframe(history_wide_df, use_container_width=True, hide_index=True)

    else:
        st.info("No rounds saved yet.")

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Undo Last Round"):
            if st.session_state.rounds:
                st.session_state.rounds.pop()
                recalculate_totals()
                st.rerun()
            else:
                st.warning("No rounds to undo.")

    with col2:
        if st.button("Reset Game"):
            st.session_state.clear()
            st.rerun()