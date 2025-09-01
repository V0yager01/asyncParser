from io import BytesIO

import pandas as pd


def parse_xls(url):
    xls = pd.ExcelFile(BytesIO(url))
    df_raw = xls.parse(xls.sheet_names[0])

    start_idx = df_raw[df_raw.astype(str).apply(
        lambda row: row.str.contains("Единица измерения: Метрическая тонна", case=False, na=False).any(), axis=1)].index[0]

    header_row = start_idx + 1
    df_table = df_raw.iloc[header_row + 1:].copy()
    df_table.columns = df_raw.iloc[header_row]
    df_table.columns = df_table.columns.str.replace('\n', ' ',).str.strip()

    end_idx = df_table[df_table.astype(str).apply(
        lambda row: row.str.contains("Итого:", case=False, na=False).any(),
        axis=1
    )].index

    if not end_idx.empty:
        df_table = df_table.loc[:end_idx[0] - 1]

    df_table['Количество Договоров, шт.'] = pd.to_numeric(
        df_table['Количество Договоров, шт.'], errors='coerce'
    )

    df_filtered = df_table[df_table['Количество Договоров, шт.'] > 0]

    return df_filtered
