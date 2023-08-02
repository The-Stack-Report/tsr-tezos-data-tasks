

def runTask():
    # Iterate over entrypoint names to get additional statistics for index
    query_counter = 0
    stats_merged = []
    entrypoints_dir_path = cache_path / "entrypoints"
    entrypoints_dir_path.mkdir(parents=True, exist_ok=True)
    transfer_counter = 0
    for entrypoint in tqdm(entrypoints_to_process):
        query_counter = query_counter + 1

        
        if False:
            print("Getting statistics for entrypoint: ",
                    entrypoint,
                    " (", query_counter, "/", len(entrypoints_to_process), ")" 
                    )

        entrypoint_stat_file_path = entrypoints_dir_path / f"tezos_entrypoints_index_{entrypoint}_{today_date}.csv"

        entrypoint_stats_df = None
        if entrypoint_stat_file_path.exists():
            # print("Using cache file")
            entrypoint_stats_df = pd.read_csv(entrypoint_stat_file_path)
        else:
            # print("qyerying db for entrypoint: ", entrypoint)
            q = entrypoint_statistics_query(entrypoint)
            logging.info(f"Querying statistics for entrypoint: {entrypoint}")
            entrypoint_stats_df = pd.read_sql(
                sql=q,
                con=dbConnection,
                params={"entrypoint": entrypoint}
                )
            entrypoint_stats_df.to_csv(entrypoint_stat_file_path, index=False)
        
        
        if entrypoint == "transfer":
            transfer_counter = transfer_counter + 1
            print(entrypoint_stats_df)

        stats_merged.append(entrypoint_stats_df)
    
    print("transfer_counter: ", transfer_counter)


    # Merge and store results to local cache.
    stats_merged_df = pd.concat(stats_merged).drop_duplicates().reset_index(drop=True)
    stats_merged_df.rename(columns={"total_rows": "calls"}, inplace=True)
    stats_merged_df.sort_values(by=['calls'], ascending=False, inplace=True)
