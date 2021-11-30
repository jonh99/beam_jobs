import logging
import argparse
import apache_beam as beam
from apache_beam.dataframe import convert
from apache_beam.io.gcp.internal.clients import bigquery


parser = argparse.ArgumentParser(description='load file')
parser.add_value_provider_argument('csv', default='gs://bucket1nba/ASA All NBA Raw Data.csv', help='paste path to nba stats csv')


with beam.Pipeline() as pipeline:
  beam_df = pipeline | 'Read CSV' >> beam.dataframe.io.read_csv(parser)
  beam_df.rename(columns={'PG%': 'PG_pct', 'SG%': 'SG_pct', 'SF%': 'SF_pct', 'PF%': 'PF_pct', 'C%': 'C_pct'}, inplace=True)
  filtered_beam_df = beam_df.loc[beam_df['did_not_play'] == 0]
  filtered_beam_df[['Inactives']].fillna(value='None', inplace='True')

table_spec = bigquery.TableReference(
    projectId='projecto-331716',
    datasetId='dataset1',
    tableId='nba-stats-staging')

#table schema, it ends in 407 rows from here
table_schema = [
  {
    "mode": "NULLABLE",
    "name": "game_id",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "game_date",
    "type": "DATE"
  },
  {
    "mode": "NULLABLE",
    "name": "OT",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "H_A",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "Team_Abbrev",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "Team_Score",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "Team_pace",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Team_efg_pct",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Team_tov_pct",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Team_orb_pct",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Team_ft_rate",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Team_off_rtg",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Inactives",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "Opponent_Abbrev",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "Opponent_Score",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "Opponent_pace",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Opponent_efg_pct",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Opponent_tov_pct",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Opponent_orb_pct",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Opponent_ft_rate",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Opponent_off_rtg",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "player",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "player_id",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "starter",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "mp",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "fg",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "fga",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "fg_pct",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "fg3",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "fg3a",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "fg3_pct",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "ft",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "fta",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "ft_pct",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "orb",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "drb",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "trb",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "ast",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "stl",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "blk",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "tov",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "pf",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "pts",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "plus_minus",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "did_not_play",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "is_inactive",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "ts_pct",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "efg_pct",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "fg3a_per_fga_pct",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "fta_per_fga_pct",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "orb_pct",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "drb_pct",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "trb_pct",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "ast_pct",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "stl_pct",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "blk_pct",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "tov_pct",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "usg_pct",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "off_rtg",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "def_rtg",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "bpm",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "season",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "minutes",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "double_double",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "triple_double",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "DKP",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "FDP",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "SDP",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "DKP_per_minute",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "FDP_per_minute",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "SDP_per_minute",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "pf_per_minute",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "ts",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "last_60_minutes_per_game_starting",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "last_60_minutes_per_game_bench",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "PG_",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "SG_",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "SF_",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "PF_",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "C_",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "active_position_minutes",
    "type": "STRING"
  }
]



(
    # Convert the Beam DataFrame to a PCollection.
    convert.to_pcollection(filtered_beam_df)



    # We get named tuples, we can convert them to dictionaries like this.
    | 'To dictionaries' >> beam.Map(lambda x: dict(x._asdict()))

    # Print the elements in the PCollection.
    #| 'Print' >> beam.Map(print)

    #WriteToBigQuery
    | 'Write to BQ' >> beam.io.WriteToBigQuery(
      table_spec,
      schema=table_schema,
      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
