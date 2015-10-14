#!/usr/bin/env python
#
# tournament.py -- implementation of a Swiss-system tournament
#

import os

import psycopg2

# If TOURNAMENT_DSN environment is set, use it
DSN = os.environ.get("TOURNAMENT_DSN", "dbname=tournament")


def connect():
    """Connect to the PostgreSQL database.  Returns a database connection."""
    return psycopg2.connect(DSN)


def deleteMatches():
    """Remove all the match records from the database."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE match RESTART IDENTITY")


def deletePlayers():
    """Remove all the player records from the database."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE player RESTART IDENTITY CASCADE")


def countPlayers():
    """Returns the number of players currently registered."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) from player")
            row = cur.fetchone()
            return row[0]


def registerPlayer(name):
    """Adds a player to the tournament database.

    The database assigns a unique serial id number for the player.  (This
    should be handled by your SQL database schema, not in your Python code.)

    Args:
      name: the player's full name (need not be unique).
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO player (name) VALUES (%s)", (name,))


def playerStandings():
    """Returns a list of the players and their win records, sorted by wins.

    The first entry in the list should be the player in first place, or a
    player tied for first place if there is currently a tie.

    Returns:
      A list of tuples, each of which contains (id, name, wins, matches):
        id: the player's unique id (assigned by the database)
        name: the player's full name (as registered)
        wins: the number of matches the player has won
        matches: the number of matches the player has played
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT player.id,
                       player.name,
                       (SELECT COUNT(*)
                            FROM match WHERE match.winner = player.id) wins,
                       (SELECT COUNT(*)
                            FROM match WHERE match.loser = player.id OR
                                             match.winner = player.id) matches
                FROM player
                ORDER BY wins DESC
                """)
            return cur.fetchall()


def reportMatch(winner, loser):
    """Records the outcome of a single match between two players.

    Args:
      winner:  the id number of the player who won
      loser:  the id number of the player who lost
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO match (winner, loser) VALUES (%s, %s)",
                (winner, loser)
            )


def swissPairings():
    """Returns a list of pairs of players for the next round of a match.

    Assuming that there are an even number of players registered, each player
    appears exactly once in the pairings.  Each player is paired with another
    player with an equal or nearly-equal win record, that is, a player adjacent
    to him or her in the standings.

    Returns:
      A list of tuples, each of which contains (id1, name1, id2, name2)
        id1: the first player's unique id
        name1: the first player's name
        id2: the second player's unique id
        name2: the second player's name
    """

    standings = playerStandings()
    matches = []

    for p1, p2 in zip(standings[::2], standings[1::2]):
        matches.append((p1[0], p1[1], p2[0], p2[1]))

    return matches
