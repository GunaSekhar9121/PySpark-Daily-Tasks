import pandas as pd

FAR_FUTURE = pd.Timestamp("9999-12-31")

def scd2_update(src_df, dim_df, natural_key, tracked_cols):
    now = pd.Timestamp.utcnow().floor("s")

    # If no dimension yet OR missing SCD2 columns → bootstrap from source
    need_bootstrap = (
        dim_df is None
        or dim_df.empty
        or any(c not in (dim_df.columns if dim_df is not None else []) 
               for c in ["start_date", "end_date", "is_current"])
    )
    if need_bootstrap:
        df = src_df.copy()
        df["start_date"] = now
        df["end_date"] = FAR_FUTURE
        df["is_current"] = 1
        return df

    # Use only current rows for comparison
    current_dim = dim_df[dim_df["is_current"] == 1].copy()

    # Join source with current on natural key; bring over tracked cols from current (as *_old)
    joined = src_df.merge(
        current_dim[[natural_key] + tracked_cols],
        on=natural_key,
        how="left",
        suffixes=("", "_old")
    )

    # Find changed keys (any tracked column differs)
    changed_keys = set()
    for col in tracked_cols:
        mask = joined[col] != joined[f"{col}_old"]
        changed_keys.update(joined.loc[mask, natural_key])

    # Find new keys (not in current_dim)
    new_keys = set(src_df[natural_key]) - set(current_dim[natural_key])

    # Expire current rows for changed keys
    expire_mask = (dim_df[natural_key].isin(changed_keys)) & (dim_df["is_current"] == 1)
    dim_df.loc[expire_mask, ["end_date", "is_current"]] = [now, 0]

    # Add new rows for new + changed keys
    to_add = src_df[src_df[natural_key].isin(new_keys | changed_keys)].copy()
    if not to_add.empty:
        to_add["start_date"] = now
        to_add["end_date"] = FAR_FUTURE
        to_add["is_current"] = 1

    return pd.concat([dim_df, to_add], ignore_index=True)
