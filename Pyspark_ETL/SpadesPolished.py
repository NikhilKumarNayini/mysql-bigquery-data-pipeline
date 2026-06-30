import streamlit as st
import pandas as pd

st.set_page_config(page_title="Spades Score Tracker", page_icon="♠", layout="wide")

BAG_LIMIT = 5
BAG_PENALTY = 50
ROUNDS = 13

st.title("♠ Spades Score Tracker")


def init_state():
    defaults = {
        "game_started": False,
        "players": [],
        "rounds": [],
        "scores": {},
        "total_bags": {},
        "penalties_applied": {},
        "edit_round": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def calculate_round_score(round_no, bid, tricks):
    if round_no == 13 and bid == 0:
        return 100 if tricks == 0 else -100
    return bid * 10 if tricks >= bid else bid * -10


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

            entry["Bag Penalty This Round"] = 0

            if new_penalties > 0:
                penalty_points = new_penalties * BAG_PENALTY
                st.session_state.scores[player] -= penalty_points
                st.session_state.penalties_applied[player] += new_penalties
                entry["Bag Penalty This Round"] = -penalty_points

            entry["Total Score After Round"] = st.session_state.scores[player]
            entry["Total Bags After Round"] = st.session_state.total_bags[player]
            entry["Bag Penalties Applied"] = st.session_state.penalties_applied[player]


def build_round_entries(round_no, existing_round=None):
    entries = []
    total_tricks = 0
    total_bids = 0

    cols = st.columns(len(st.session_state.players))

    for idx, player in enumerate(st.session_state.players):
        old = None
        if existing_round:
            old = next((e for e in existing_round if e["Player"] == player), None)

        with cols[idx]:
            st.markdown(f"### {player}")

            bid = st.number_input(
                "Bid",
                min_value=0,
                max_value=round_no,
                value=old["Bid"] if old else 0,
                step=1,
                key=f"bid_{round_no}_{player}_{st.session_state.edit_round}",
            )

            tricks = st.number_input(
                "Tricks Won",
                min_value=0,
                max_value=round_no,
                value=old["Tricks Won"] if old else 0,
                step=1,
                key=f"tricks_{round_no}_{player}_{st.session_state.edit_round}",
            )

            round_score = calculate_round_score(round_no, bid, tricks)
            bags_this_round = calculate_bags(bid, tricks)

            total_tricks += tricks
            total_bids += bid

            st.write(f"Score: **{round_score}**")
            st.write(f"Bags: **{bags_this_round}**")

            entries.append({
                "Round": round_no,
                "Player": player,
                "Bid": bid,
                "Tricks Won": tricks,
                "Round Score": round_score,
                "Bags This Round": bags_this_round,
                "Bag Penalty This Round": 0,
                "Total Score After Round": 0,
                "Total Bags After Round": 0,
                "Bag Penalties Applied": 0,
            })

    return entries, total_tricks, total_bids


def show_leaderboard():
    st.subheader("🏆 Current Leaderboard")

    df = pd.DataFrame([
        {
            "Rank": 0,
            "Player": p,
            "Total Score": st.session_state.scores[p],
            "Total Bags": st.session_state.total_bags[p],
            "Bag Penalties": st.session_state.penalties_applied[p],
        }
        for p in st.session_state.players
    ])

    df = df.sort_values("Total Score", ascending=False).reset_index(drop=True)
    df["Rank"] = df.index + 1

    st.dataframe(df, use_container_width=True, hide_index=True)


def show_score_grid():
    st.subheader("📊 Round-wise Scoreboard")

    if not st.session_state.rounds:
        st.info("No rounds saved yet.")
        return

    rows = []

    for player in st.session_state.players:
        row = {"Player": player}

        for round_data in st.session_state.rounds:
            r = round_data[0]["Round"]
            entry = next(e for e in round_data if e["Player"] == player)

            score = entry["Round Score"]
            bags = entry["Bags This Round"]
            penalty = entry["Bag Penalty This Round"]

            cell = f"{score}"
            if bags:
                cell += f" | Bags: {bags}"
            if penalty:
                cell += f" | Penalty: {penalty}"

            row[f"R{r}"] = cell

        row["Total"] = st.session_state.scores[player]
        row["Bags"] = st.session_state.total_bags[player]

        rows.append(row)

    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def show_round_controls():
    st.subheader("⚙️ Controls")

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("↩️ Undo Last Round"):
            if st.session_state.rounds:
                st.session_state.rounds.pop()
                st.session_state.edit_round = None
                recalculate_totals()
                st.rerun()
            else:
                st.warning("No rounds to undo.")

    with col2:
        if st.session_state.rounds:
            selected_round = st.selectbox(
                "Edit saved round",
                options=[r[0]["Round"] for r in st.session_state.rounds],
            )
            if st.button("✏️ Edit Round"):
                st.session_state.edit_round = selected_round
                st.rerun()

    with col3:
        if st.button("🔄 Reset Game"):
            st.session_state.clear()
            st.rerun()


init_state()

if not st.session_state.game_started:
    st.subheader("Game Setup")

    num_players = st.number_input("Number of players", min_value=2, max_value=10, value=5, step=1)

    players = []
    for i in range(num_players):
        name = st.text_input(f"Player {i + 1} name", value=f"Player {i + 1}")
        players.append(name.strip())

    if st.button("Start Game"):
        if any(not p for p in players):
            st.error("Player names cannot be empty.")
        elif len(set(players)) != len(players):
            st.error("Player names must be unique.")
        else:
            st.session_state.players = players
            st.session_state.rounds = []
            st.session_state.scores = {p: 0 for p in players}
            st.session_state.total_bags = {p: 0 for p in players}
            st.session_state.penalties_applied = {p: 0 for p in players}
            st.session_state.game_started = True
            st.rerun()

else:
    if st.session_state.edit_round:
        round_no = st.session_state.edit_round
        existing_round = st.session_state.rounds[round_no - 1]

        st.subheader(f"✏️ Editing Round {round_no}")
        entries, total_tricks, total_bids = build_round_entries(round_no, existing_round)

        st.info(f"Total bids: {total_bids} | Total tricks entered: {total_tricks}/{round_no}")

        if total_tricks != round_no:
            st.warning(f"Total tricks must equal {round_no}.")
        if total_bids < round_no:
            st.warning("Total bids are less than available tricks.")

        col1, col2 = st.columns(2)

        with col1:
            if st.button("💾 Save Edited Round"):
                if total_tricks != round_no:
                    st.error(f"Invalid entry. Total tricks must equal {round_no}.")
                else:
                    st.session_state.rounds[round_no - 1] = entries
                    st.session_state.edit_round = None
                    recalculate_totals()
                    st.rerun()

        with col2:
            if st.button("Cancel Edit"):
                st.session_state.edit_round = None
                st.rerun()

    else:
        current_round = len(st.session_state.rounds) + 1

        if current_round <= ROUNDS:
            st.subheader(f"Enter Round {current_round} Scores")
            st.caption(f"Cards dealt / total tricks this round: {current_round}")

            entries, total_tricks, total_bids = build_round_entries(current_round)

            st.info(f"Total bids: {total_bids} | Total tricks entered: {total_tricks}/{current_round}")

            if total_tricks != current_round:
                st.warning(f"Total tricks must equal {current_round}.")
            if total_bids < current_round:
                st.warning("Total bids are less than available tricks.")

            if st.button("💾 Save Round"):
                if total_tricks != current_round:
                    st.error(
                        f"Invalid entry. In Round {current_round}, total tricks won by all players "
                        f"must be exactly {current_round}. You entered {total_tricks}."
                    )
                else:
                    st.session_state.rounds.append(entries)
                    recalculate_totals()
                    st.rerun()
        else:
            st.success("Game Completed!")
            winner = max(st.session_state.scores, key=st.session_state.scores.get)
            st.header(f"🏆 {winner} Wins!")

    st.divider()
    show_leaderboard()

    st.divider()
    show_score_grid()

    st.divider()
    show_round_controls()