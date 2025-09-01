from io import BytesIO

import pandas as pd


def parse_xls(xls_bytes):
    xls = pd.ExcelFile(BytesIO(xls_bytes))
    df_raw = xls.parse(xls.sheet_names[0])

    row_strings = df_raw.astype(str).agg(' '.join, axis=1)

    start_idx = row_strings[row_strings.str.contains("Единица измерения: Метрическая тонна", case=False)].index[0]
    header_row = start_idx + 1

    df_table = df_raw.iloc[header_row + 1:].copy()
    df_table.columns = df_raw.iloc[header_row]
    df_table.columns = df_table.columns.str.replace('\n', ' ').str.strip()

    end_idx_candidates = row_strings[row_strings.str.contains("Итого:", case=False)].index
    end_idx = [i for i in end_idx_candidates if i > header_row]
    if end_idx:
        df_table = df_table.loc[:end_idx[0] - header_row - 2]

    df_table['Количество Договоров, шт.'] = pd.to_numeric(df_table['Количество Договоров, шт.'], errors='coerce')
    df_filtered = df_table[df_table['Количество Договоров, шт.'] > 0]

    return df_filtered