from __future__ import annotations

from datetime import date, datetime, timedelta
from io import BytesIO, StringIO
from typing import Any
import ftplib
import os

import numpy as np
import pandas as pd
from django.core.files.base import ContentFile

from core.models import ProcessingRun

MAX_PREVIEW_ROWS = 30
FTP_TIMEOUT_SECONDS = 10
DEFAULT_LOOKBACK_DAYS = 4

FTP_CONFIG = {
    'HOST': os.getenv('LEGACY_FTP_HOST', '10.200.251.20'),
    'USER': os.getenv('LEGACY_FTP_USER', 'ftpuser'),
    'PASS': os.getenv('LEGACY_FTP_PASS', 'utec.2024'),
    'DIR_LUCAS': os.getenv('LEGACY_FTP_DIR_LUCAS', '/home/ftpuser/Lucas-Tesina'),
    'DIR_MEM': os.getenv('LEGACY_FTP_DIR_MEM', '/home/ftpuser/23243567/MemFlash'),
}

CAUDAL_AIRE_M3H = 750.0
CURRENT_DAY_FREQ = '1min'
HISTORY_FREQ = '5min'
ENERGY_FREQ = '1min'
ALLOWED_FREQS = {'1min', '5min', '15min', '30min', '1h'}


# -------------------------------------------------------------
# Sanitização
# -------------------------------------------------------------
def _safe_float(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, 'item'):
        try:
            return value.item()
        except Exception:
            pass
    return value


def _sanitize_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if hasattr(value, 'isoformat'):
        try:
            return value.isoformat()
        except Exception:
            pass
    return _safe_float(value)


def _sanitize_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{str(k): _sanitize_value(v) for k, v in row.items()} for row in records]


def _sanitize_dict(data: dict[str, Any]) -> dict[str, Any]:
    return {str(k): _sanitize_value(v) for k, v in data.items()}


# -------------------------------------------------------------
# Cálculos físicos herdados do monitor.py
# -------------------------------------------------------------
def calcular_cp_agua(t_celsius: pd.Series | float) -> pd.Series | float:
    return (
        4.2174
        - (0.00372 * t_celsius)
        + (1.4128e-4 * (t_celsius ** 2))
        - (2.654e-6 * (t_celsius ** 3))
        + (2.093e-8 * (t_celsius ** 4))
    )


def calcular_cp_aire(t_celsius: pd.Series | float) -> pd.Series | float:
    return 1.0038 + (0.00016 * t_celsius)


def calcular_rho_aire(t_celsius: pd.Series | float) -> pd.Series | float:
    t_kelvin = t_celsius + 273.15
    return 101325.0 / (287.05 * t_kelvin)


# -------------------------------------------------------------
# Aquisição FTP legado
# -------------------------------------------------------------
def get_ftp_file(ftp: ftplib.FTP, directory: str, filename_contains: str) -> BytesIO | None:
    try:
        ftp.cwd(directory)
        files = ftp.nlst()
        target = next((f for f in files if filename_contains in f), None)
        if not target:
            return None
        buff = BytesIO()
        ftp.retrbinary(f'RETR {target}', buff.write)
        buff.seek(0)
        return buff
    except Exception:
        return None


def process_lucas_data(buff: BytesIO | None) -> pd.DataFrame:
    if not buff:
        return pd.DataFrame()

    df = pd.read_csv(buff, sep=';', decimal=',')
    df.columns = [c.strip().replace('"', '').replace("'", '') for c in df.columns]

    for col in df.columns:
        if df[col].dtype == object:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace('"', '', regex=False)
                .str.replace("'", '', regex=False)
                .str.strip()
            )

    df['Dt'] = pd.to_datetime(df['Fecha'] + ' ' + df['Hora'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['Dt']).set_index('Dt').sort_index()

    cols_to_num = [
        'T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8', 'Caudal',
        'Humedad_S1', 'Humedad_S2', 'R1', 'R2', 'R3', 'R4',
    ]
    for c in cols_to_num:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')

    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        df[numeric_cols] = df[numeric_cols].interpolate(method='linear', limit_direction='both')

    return df.rename(columns={'T1': 'T1_LUCAS'})


def process_mem_data(buff: BytesIO | None) -> pd.DataFrame:
    if not buff:
        return pd.DataFrame()

    df = pd.read_csv(buff, sep=';', skiprows=1, dtype=str)
    df.columns = [c.strip().replace('"', '').replace("'", '') for c in df.columns]

    for col in df.columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace('"', '', regex=False)
            .str.replace("'", '', regex=False)
            .str.strip()
        )

    if 'DATE' not in df.columns or 'TIME' not in df.columns:
        return pd.DataFrame()

    df['Dt'] = pd.to_datetime(df['DATE'] + ' ' + df['TIME'], format='%m/%d/%Y %H:%M:%S', errors='coerce')
    df = df.dropna(subset=['Dt']).set_index('Dt').sort_index()

    for c in ['T1', 'GHI']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].str.replace(',', '.', regex=False), errors='coerce')

    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) == 0:
        return pd.DataFrame()

    df = df[numeric_cols].resample('1min').mean().interpolate(method='linear')
    df = df.rename(columns={'T1': 'T1_MEM'})

    cols_return = [c for c in ['T1_MEM', 'GHI'] if c in df.columns]
    return df[cols_return]


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return pd.to_datetime(value).date()
    except Exception:
        return None


def _resolve_date_window(params: dict[str, Any]) -> tuple[date, date]:
    data_inicio = _parse_date(params.get('data_inicio'))
    data_fim = _parse_date(params.get('data_fim'))
    today = datetime.now().date()

    if data_inicio and data_fim and data_fim < data_inicio:
        raise ValueError('A data final não pode ser anterior à data inicial.')

    if data_inicio and not data_fim:
        data_fim = data_inicio
    if data_fim and not data_inicio:
        data_inicio = data_fim

    if not data_inicio and not data_fim:
        data_fim = today
        data_inicio = today - timedelta(days=DEFAULT_LOOKBACK_DAYS - 1)

    return data_inicio, data_fim


def buscar_dados_legacy_impl(params: dict[str, Any]) -> pd.DataFrame:
    data_inicio, data_fim = _resolve_date_window(params)
    dias_total = (data_fim - data_inicio).days + 1

    ftp = ftplib.FTP(FTP_CONFIG['HOST'], FTP_CONFIG['USER'], FTP_CONFIG['PASS'], timeout=FTP_TIMEOUT_SECONDS)
    try:
        df_lucas_list: list[pd.DataFrame] = []
        for offset in range(dias_total):
            target_date = data_inicio + timedelta(days=offset)
            fname_lucas = target_date.strftime('%Y_%m_%d') + '_datos.csv'
            buff_lucas = get_ftp_file(ftp, FTP_CONFIG['DIR_LUCAS'], fname_lucas)
            if buff_lucas:
                df_day = process_lucas_data(buff_lucas)
                if not df_day.empty:
                    df_lucas_list.append(df_day)

        if df_lucas_list:
            df_lucas = pd.concat(df_lucas_list)
            df_lucas = df_lucas[~df_lucas.index.duplicated(keep='last')].sort_index()
        else:
            df_lucas = pd.DataFrame()

        buff_mem = get_ftp_file(ftp, FTP_CONFIG['DIR_MEM'], 'MemFlash.txt')
        df_mem = process_mem_data(buff_mem)
    finally:
        try:
            ftp.quit()
        except Exception:
            pass

    if not df_lucas.empty and not df_mem.empty:
        df_final = pd.merge_asof(
            df_lucas,
            df_mem,
            left_index=True,
            right_index=True,
            tolerance=pd.Timedelta('2min'),
            direction='nearest',
        )
    elif not df_lucas.empty:
        df_final = df_lucas.copy()
        df_final['GHI'] = np.nan
        df_final['T1_MEM'] = np.nan
    else:
        return pd.DataFrame()

    mask = (df_final.index.date >= data_inicio) & (df_final.index.date <= data_fim)
    df_final = df_final.loc[mask].copy()
    if df_final.empty:
        return pd.DataFrame()

    df_final = df_final.infer_objects(copy=False)
    df_final = df_final.resample('1s').asfreq()
    numeric_cols = df_final.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        df_final[numeric_cols] = df_final[numeric_cols].interpolate(method='time')

    cols_calc = ['T1_LUCAS', 'T4', 'T5', 'T6', 'T7', 'T8', 'Caudal', 'R2', 'R3']
    for c in cols_calc:
        if c not in df_final.columns:
            df_final[c] = 0.0

    is_pump_on = df_final['R2'] > 0.5
    is_flowing = df_final['Caudal'] > 0.5
    active_flow = is_pump_on & is_flowing

    m_dot_water = df_final['Caudal'] / 60.0

    t_med_col = (df_final['T1_LUCAS'] + df_final['T6']) / 2.0
    t_med_aero_w = (df_final['T4'] + df_final['T5']) / 2.0
    t_med_t1_t5 = (df_final['T1_LUCAS'] + df_final['T5']) / 2.0
    t_med_aero_a = (df_final['T8'] + df_final['T7']) / 2.0

    cp_agua_col = calcular_cp_agua(t_med_col)
    cp_agua_aero = calcular_cp_agua(t_med_aero_w)
    cp_agua_t1_t5 = calcular_cp_agua(t_med_t1_t5)
    cp_aire_aero = calcular_cp_aire(t_med_aero_a)
    rho_aire_aero = calcular_rho_aire(t_med_aero_a)

    df_final['Q_COLECTOR'] = np.where(active_flow, m_dot_water * cp_agua_col * (df_final['T1_LUCAS'] - df_final['T6']), 0)
    df_final['Q_AERO_W'] = np.where(active_flow, m_dot_water * cp_agua_aero * (df_final['T4'] - df_final['T5']), 0)
    df_final['Q_T1_T5'] = np.where(active_flow, m_dot_water * cp_agua_t1_t5 * (df_final['T1_LUCAS'] - df_final['T5']), 0)

    m_dot_air = (CAUDAL_AIRE_M3H / 3600.0) * rho_aire_aero
    has_t8 = df_final['T8'] > 0
    is_fan_on = df_final['R3'] > 0.5
    df_final['Q_AERO_A'] = np.where((has_t8 & is_fan_on), m_dot_air * cp_aire_aero * (df_final['T8'] - df_final['T7']), 0)

    potencia_colector_positiva = np.clip(df_final['Q_COLECTOR'], 0, None)
    df_final['Energia_Colector_kWh'] = potencia_colector_positiva * (1.0 / 3600.0)

    return df_final


# -------------------------------------------------------------
# Persistência / leitura do dataset salvo na execução
# -------------------------------------------------------------
def _serialize_dataframe_csv(df: pd.DataFrame) -> bytes:
    out = StringIO()
    serial = df.reset_index().rename(columns={df.index.name or 'index': 'timestamp'})
    serial.to_csv(out, index=False)
    return out.getvalue().encode('utf-8')


def salvar_dataframe_execucao(execucao: ProcessingRun, df: pd.DataFrame) -> None:
    filename = f"execucao_{execucao.pk}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    content = ContentFile(_serialize_dataframe_csv(df))
    if execucao.arquivo:
        try:
            execucao.arquivo.delete(save=False)
        except Exception:
            pass
    execucao.arquivo.save(filename, content, save=False)


def carregar_execucao_dataframe(execucao: ProcessingRun) -> pd.DataFrame:
    if execucao.arquivo:
        execucao.arquivo.open('rb')
        try:
            df = pd.read_csv(execucao.arquivo)
        finally:
            execucao.arquivo.close()
        if 'timestamp' not in df.columns:
            raise ValueError('O CSV salvo na execução não possui a coluna timestamp.')
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df = df.dropna(subset=['timestamp']).set_index('timestamp').sort_index()
        for col in df.columns:
            if df[col].dtype == object:
                try:
                    df[col] = pd.to_numeric(df[col], errors='ignore')
                except Exception:
                    pass
        return df

    return buscar_dados_legacy_impl(execucao.parametros_busca or {})


# -------------------------------------------------------------
# Helpers de gráfico / filtro
# -------------------------------------------------------------
def _resample_numeric(df: pd.DataFrame, columns: list[str], freq: str) -> pd.DataFrame:
    cols = [c for c in columns if c in df.columns]
    if not cols or df.empty:
        return pd.DataFrame(index=df.index[:0])
    out = df[cols].resample(freq).mean()
    return out.interpolate(method='time', limit_direction='both')


def _resample_step(df: pd.DataFrame, columns: list[str], freq: str) -> pd.DataFrame:
    cols = [c for c in columns if c in df.columns]
    if not cols or df.empty:
        return pd.DataFrame(index=df.index[:0])
    return df[cols].ffill().resample(freq).max().fillna(0)


def _chart_payload(df: pd.DataFrame, label_fmt: str, series_names: dict[str, str] | None = None) -> dict[str, Any]:
    if df.empty:
        return {'labels': [], 'series': {}}
    payload = {'labels': [ts.strftime(label_fmt) for ts in df.index], 'series': {}}
    for col in df.columns:
        key = series_names.get(col, col) if series_names else col
        payload['series'][key] = [None if pd.isna(v) else float(v) for v in df[col].tolist()]
    return payload


def _relay_duration_text(df: pd.DataFrame, col: str) -> str:
    if col not in df.columns or df.empty:
        return '0s'
    series = df[col].dropna()
    if series.empty:
        return '0s'
    current_val = int(series.iloc[-1] > 0.5)
    idx = series.index
    last_change = idx[0]
    for i in range(len(series) - 1, -1, -1):
        val = int(series.iloc[i] > 0.5)
        if val != current_val:
            last_change = idx[i + 1] if i + 1 < len(idx) else idx[-1]
            break
        last_change = idx[i]
    duration = idx[-1] - last_change
    total_seconds = int(duration.total_seconds())
    hours, rem = divmod(total_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    if hours:
        return f'{hours}h {minutes:02d}m'
    if minutes:
        return f'{minutes}m {seconds:02d}s'
    return f'{seconds}s'


def _latest_value(df: pd.DataFrame, col: str) -> Any:
    if col not in df.columns:
        return None
    valid = df[col].dropna()
    if valid.empty:
        return None
    return _sanitize_value(valid.iloc[-1])


def _normalize_freq(freq: str | None, default: str = HISTORY_FREQ) -> str:
    if not freq:
        return default
    freq = freq.strip().lower()
    if freq not in ALLOWED_FREQS:
        return default
    return freq


def _parse_dt(value: str | None) -> pd.Timestamp | None:
    if not value:
        return None
    try:
        ts = pd.to_datetime(value)
        if pd.isna(ts):
            return None
        return ts
    except Exception:
        return None


def filtrar_dataframe(df: pd.DataFrame, start: str | None = None, end: str | None = None) -> pd.DataFrame:
    if df.empty:
        return df
    start_ts = _parse_dt(start)
    end_ts = _parse_dt(end)
    out = df.copy()
    if start_ts is not None:
        out = out[out.index >= start_ts]
    if end_ts is not None:
        out = out[out.index <= end_ts]
    return out


def _build_relay_cards(df: pd.DataFrame) -> list[dict[str, Any]]:
    relay_cards: list[dict[str, Any]] = []
    relay_labels = {'R1': 'R1', 'R2': 'Bomba (R2)', 'R3': 'Ventilador (R3)', 'R4': 'R4'}
    for col in ['R1', 'R2', 'R3', 'R4']:
        if col in df.columns:
            latest = _latest_value(df, col)
            state = 1 if (latest or 0) > 0.5 else 0
            relay_cards.append({
                'name': relay_labels[col],
                'state': 'ON' if state else 'OFF',
                'duration': _relay_duration_text(df, col),
                'value': latest,
            })
    return relay_cards


def construir_dashboard(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {}

    end_ts = df.index.max()
    day_start = end_ts.replace(hour=0, minute=0, second=0, microsecond=0)
    hist_start = end_ts - timedelta(days=3)

    df_today = df[df.index >= day_start].copy()
    df_hist = df[df.index >= hist_start].copy()

    current_left = _resample_numeric(df_today, ['T1_LUCAS', 'T2', 'T3', 'T6'], CURRENT_DAY_FREQ)
    current_right = _resample_numeric(df_today, ['T4', 'T5', 'T7', 'T8'], CURRENT_DAY_FREQ)
    current_hum = _resample_numeric(df_today, ['Humedad_S1', 'Humedad_S2'], CURRENT_DAY_FREQ)
    current_flow = _resample_numeric(df_today, ['Caudal'], CURRENT_DAY_FREQ)
    current_relays = _resample_step(df_today, ['R1', 'R2', 'R3', 'R4'], CURRENT_DAY_FREQ)

    hist_temp = _resample_numeric(df_hist, ['T1_LUCAS', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8'], HISTORY_FREQ)
    hist_hum = _resample_numeric(df_hist, ['Humedad_S1', 'Humedad_S2'], HISTORY_FREQ)
    hist_flow = _resample_numeric(df_hist, ['Caudal'], HISTORY_FREQ)
    hist_relays = _resample_step(df_hist, ['R1', 'R2', 'R3', 'R4'], HISTORY_FREQ)

    energy_cols = [c for c in ['Q_COLECTOR', 'Q_T1_T5', 'Q_AERO_W', 'Q_AERO_A'] if c in df_today.columns]
    energy_source = df_today.copy()
    if energy_cols:
        mask_positive = (energy_source[energy_cols] > 0).any(axis=1)
        if mask_positive.any():
            first_positive = energy_source[mask_positive].index[0] - pd.Timedelta(minutes=10)
            first_positive = max(first_positive, day_start)
            energy_source = energy_source[energy_source.index >= first_positive]
    energy_numeric = _resample_numeric(energy_source, energy_cols, ENERGY_FREQ)
    if not energy_numeric.empty:
        energy_numeric = energy_numeric.clip(lower=0)
    energy_relays = _resample_step(energy_source, ['R2'], ENERGY_FREQ)
    if not energy_relays.empty and not energy_numeric.empty:
        energy_numeric['R2'] = energy_relays['R2']

    latest_cards = {
        'T1': _latest_value(df_today, 'T1_LUCAS'),
        'T2': _latest_value(df_today, 'T2'),
        'T3': _latest_value(df_today, 'T3'),
        'T4': _latest_value(df_today, 'T4'),
        'T5': _latest_value(df_today, 'T5'),
        'T6': _latest_value(df_today, 'T6'),
        'T7': _latest_value(df_today, 'T7'),
        'T8': _latest_value(df_today, 'T8'),
        'Caudal': _latest_value(df_today, 'Caudal'),
        'GHI': _latest_value(df_today, 'GHI'),
        'Q_COLECTOR': _latest_value(df_today, 'Q_COLECTOR'),
        'Q_AERO_W': _latest_value(df_today, 'Q_AERO_W'),
        'Q_T1_T5': _latest_value(df_today, 'Q_T1_T5'),
        'Q_AERO_A': _latest_value(df_today, 'Q_AERO_A'),
    }

    energia_total = 0.0
    if 'Energia_Colector_kWh' in df_today.columns:
        energia_total = float(df_today['Energia_Colector_kWh'].fillna(0).sum())

    return {
        'kpis': {
            'ultima_atualizacao': _sanitize_value(end_ts),
            'energia_colector_kwh_hoje': round(energia_total, 4),
            'ghi_max_hoje': _sanitize_value(df_today['GHI'].max()) if 'GHI' in df_today.columns and df_today['GHI'].notna().any() else None,
            'caudal_max_hoje': _sanitize_value(df_today['Caudal'].max()) if 'Caudal' in df_today.columns and df_today['Caudal'].notna().any() else None,
        },
        'latest_cards': latest_cards,
        'relay_cards': _build_relay_cards(df_today),
        'current_day': {
            'left_temp': _chart_payload(current_left, '%H:%M', {'T1_LUCAS': 'T1'}),
            'right_temp': _chart_payload(current_right, '%H:%M'),
            'humidity': _chart_payload(current_hum, '%H:%M', {'Humedad_S1': 'H1', 'Humedad_S2': 'H2'}),
            'flow': _chart_payload(current_flow, '%H:%M'),
            'relays': _chart_payload(current_relays, '%H:%M'),
        },
        'history_3d': {
            'temperature': _chart_payload(hist_temp, '%d/%m %H:%M', {'T1_LUCAS': 'T1'}),
            'humidity': _chart_payload(hist_hum, '%d/%m %H:%M', {'Humedad_S1': 'H1', 'Humedad_S2': 'H2'}),
            'flow': _chart_payload(hist_flow, '%d/%m %H:%M'),
            'relays': _chart_payload(hist_relays, '%d/%m %H:%M'),
        },
        'energy_balance': {
            'power': _chart_payload(energy_numeric, '%H:%M'),
            'energia_colector_kwh_hoje': round(energia_total, 4),
        },
    }


def construir_historico_filtrado(df: pd.DataFrame, freq: str = HISTORY_FREQ, start: str | None = None, end: str | None = None) -> dict[str, Any]:
    df_filtered = filtrar_dataframe(df, start=start, end=end)
    freq = _normalize_freq(freq)
    if df_filtered.empty:
        return {
            'meta': {
                'start': start,
                'end': end,
                'freq': freq,
                'n_registros': 0,
                'janela': None,
            },
            'charts': {
                'temperature': {'labels': [], 'series': {}},
                'humidity': {'labels': [], 'series': {}},
                'flow': {'labels': [], 'series': {}},
                'relays': {'labels': [], 'series': {}},
                'energy': {'labels': [], 'series': {}},
            },
            'preview': [],
            'relay_cards': [],
            'latest_cards': {},
        }

    temp = _resample_numeric(df_filtered, ['T1_LUCAS', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8'], freq)
    hum = _resample_numeric(df_filtered, ['Humedad_S1', 'Humedad_S2'], freq)
    flow = _resample_numeric(df_filtered, ['Caudal'], freq)
    relays = _resample_step(df_filtered, ['R1', 'R2', 'R3', 'R4'], freq)
    energy_cols = [c for c in ['Q_COLECTOR', 'Q_T1_T5', 'Q_AERO_W', 'Q_AERO_A'] if c in df_filtered.columns]
    energy = _resample_numeric(df_filtered, energy_cols, freq)
    if not energy.empty:
        energy = energy.clip(lower=0)

    return {
        'meta': {
            'start': _sanitize_value(df_filtered.index.min()),
            'end': _sanitize_value(df_filtered.index.max()),
            'freq': freq,
            'n_registros': int(df_filtered.shape[0]),
            'janela': f"{_sanitize_value(df_filtered.index.min())} até {_sanitize_value(df_filtered.index.max())}",
        },
        'charts': {
            'temperature': _chart_payload(temp, '%d/%m %H:%M', {'T1_LUCAS': 'T1'}),
            'humidity': _chart_payload(hum, '%d/%m %H:%M', {'Humedad_S1': 'H1', 'Humedad_S2': 'H2'}),
            'flow': _chart_payload(flow, '%d/%m %H:%M'),
            'relays': _chart_payload(relays, '%d/%m %H:%M'),
            'energy': _chart_payload(energy, '%d/%m %H:%M'),
        },
        'preview': _sanitize_records(
            df_filtered.reset_index().rename(columns={df_filtered.index.name or 'index': 'timestamp'}).tail(60).to_dict(orient='records')
        ),
        'relay_cards': _build_relay_cards(df_filtered),
        'latest_cards': {
            'T1': _latest_value(df_filtered, 'T1_LUCAS'),
            'T2': _latest_value(df_filtered, 'T2'),
            'T3': _latest_value(df_filtered, 'T3'),
            'T4': _latest_value(df_filtered, 'T4'),
            'T5': _latest_value(df_filtered, 'T5'),
            'T6': _latest_value(df_filtered, 'T6'),
            'T7': _latest_value(df_filtered, 'T7'),
            'T8': _latest_value(df_filtered, 'T8'),
            'Caudal': _latest_value(df_filtered, 'Caudal'),
            'GHI': _latest_value(df_filtered, 'GHI'),
        },
    }


def construir_resumo(df: pd.DataFrame) -> dict[str, Any]:
    resumo: dict[str, Any] = {
        'n_linhas': int(df.shape[0]),
        'n_colunas': int(df.shape[1]),
        'colunas': [str(col) for col in df.columns],
        'nulos_por_coluna': {str(k): int(v) for k, v in df.isna().sum().to_dict().items()},
        'tipos': {str(k): str(v) for k, v in df.dtypes.astype(str).to_dict().items()},
        'preview': _sanitize_records(
            df.reset_index().rename(columns={df.index.name or 'index': 'timestamp'}).head(MAX_PREVIEW_ROWS).to_dict(orient='records')
        ),
        'ultima_timestamp': _sanitize_value(df.index.max()) if not df.empty else None,
        'primeira_timestamp': _sanitize_value(df.index.min()) if not df.empty else None,
    }

    if not df.empty:
        resumo['janela_temporal'] = {
            'inicio': _sanitize_value(df.index.min()),
            'fim': _sanitize_value(df.index.max()),
            'duracao_segundos': int((df.index.max() - df.index.min()).total_seconds()),
        }

    colunas_numericas = df.select_dtypes(include='number')
    if not colunas_numericas.empty:
        resumo['estatisticas_numericas'] = {
            coluna: _sanitize_dict(colunas_numericas[coluna].describe().to_dict())
            for coluna in colunas_numericas.columns
        }
    else:
        resumo['estatisticas_numericas'] = {}

    metricas = {}
    for col in ['T1_LUCAS', 'T1_MEM', 'GHI', 'Caudal', 'Q_COLECTOR', 'Q_AERO_W', 'Q_T1_T5', 'Q_AERO_A']:
        if col in df.columns and df[col].notna().any():
            metricas[f'{col}_ultimo'] = _sanitize_value(df[col].dropna().iloc[-1])
            metricas[f'{col}_max'] = _sanitize_value(df[col].max())

    if 'Energia_Colector_kWh' in df.columns and df['Energia_Colector_kWh'].notna().any():
        metricas['energia_colector_kwh_total'] = float(df['Energia_Colector_kWh'].fillna(0).sum())

    resumo['metricas_principais'] = metricas
    resumo['dashboard'] = construir_dashboard(df)
    return resumo


def obter_payload_tempo_real() -> dict[str, Any]:
    today = datetime.now().date()
    df = buscar_dados_legacy_impl({'data_inicio': today.isoformat(), 'data_fim': today.isoformat()})
    if df.empty:
        return {'empty': True, 'message': 'Nenhum dado disponível para o dia atual.'}
    dashboard = construir_dashboard(df)
    return {
        'empty': False,
        'auto_refresh_seconds': 300,
        'captured_at': _sanitize_value(datetime.now()),
        'dashboard': dashboard,
    }


def processar_execucao_servidor(execucao: ProcessingRun) -> ProcessingRun:
    try:
        df = buscar_dados_legacy_impl(execucao.parametros_busca or {})
        if df.empty:
            raise ValueError('Nenhum dado foi encontrado no FTP legado para a janela solicitada.')

        resumo = construir_resumo(df)
        salvar_dataframe_execucao(execucao, df)

        execucao.status = ProcessingRun.Status.SUCCESS
        execucao.total_linhas = resumo['n_linhas']
        execucao.total_colunas = resumo['n_colunas']
        execucao.colunas = resumo['colunas']
        execucao.resumo = resumo
        execucao.erro = ''
        execucao.save(
            update_fields=[
                'status',
                'total_linhas',
                'total_colunas',
                'colunas',
                'resumo',
                'erro',
                'arquivo',
                'atualizado_em',
            ]
        )
    except Exception as exc:
        execucao.status = ProcessingRun.Status.ERROR
        execucao.erro = str(exc)
        execucao.save(update_fields=['status', 'erro', 'atualizado_em'])

    return execucao
