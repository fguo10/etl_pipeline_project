"""
User-related feature derivation:

    - Calculate the number of experiments each user participated in.
    - Calculate the total run time of experiments for each user.
    - Calculate the number of days since each user signed up.
"""

import pandas as pd
def user_feature_derivation(users_df, experiments_df):
    # 1. Calculate the number of experiments each user participated in
    experiments_per_user = experiments_df.groupby('user_id').size() \
        .rename('experiments_count')

    # 2. Calculate the total run time of experiments for each user
    total_run_time_per_user = experiments_df.groupby('user_id')['experiment_run_time'].sum()

    # 3. Calculate the number of days since each user signed up
    current_date = pd.to_datetime('today')
    users_df['signup_date'] = pd.to_datetime(users_df['signup_date'])
    users_df['days_since_signup'] = (current_date - users_df['signup_date']).dt.days
    # Set any negative values in days_since_signup to 0
    users_df['days_since_signup'] = users_df['days_since_signup'].clip(lower=0)

    # Merge the results
    # Merge users_df with experiments_per_user on 'user_id'
    merged_df = users_df.merge(experiments_per_user, on='user_id', how='left')

    # Merge merged_df with total_run_time_per_user on 'user_id'
    user_features_df = merged_df.merge(total_run_time_per_user, on='user_id', how='left')

    return user_features_df
