import base64
import json
import re
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import streamlit as st
from openpyxl import Workbook
from openpyxl.drawing.image import Image as XLImage
from openpyxl.styles import Alignment, Font
from openpyxl.utils.dataframe import dataframe_to_rows

# =========================
# Branding
# =========================
EMAIL_REGEX = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)

KC_PRIMARY = "#B04EF0"
KC_ACCENT = "#E060F0"
KC_DEEP = "#8030F0"
KC_SOFT = "#F6F0FF"

EXCLUDED_OWNER_CANON = "pipedrive krispcall"
CREDIT_EXCLUDE_DESCS = {"purchased credit", "credit purchased", "amount recharged"}

# =========================
# Secrets + UI helpers
# =========================
def _get_secret(path: List[str], default=None):
    cur = st.secrets
    for key in path:
        if key not in cur:
            return default
        cur = cur[key]
    return cur

def _inject_brand_css():
    st.markdown(
        f"""
        <style>
          .kc-hero {{ padding: 18px 18px; border-radius: 18px; background: linear-gradient(90deg, {KC_DEEP} 0%, {KC_PRIMARY} 45%, {KC_ACCENT} 100%); color: white; box-shadow: 0 10px 30px rgba(0,0,0,0.08); }}
          .kc-hero h1 {{ margin: 0; font-size: 28px; line-height: 1.2; }}
          .kc-hero p {{ margin: 6px 0 0 0; opacity: 0.95; font-size: 14px; }}
          .kc-card {{ background: white; border: 1px solid rgba(176, 78, 240, 0.18); border-radius: 16px; padding: 14px 14px; box-shadow: 0 10px 24px rgba(20, 6, 31, 0.04); }}
          div.stButton > button {{ border-radius: 14px !important; border: 0 !important; background: linear-gradient(90deg, {KC_DEEP} 0%, {KC_PRIMARY} 55%, {KC_ACCENT} 100%) !important; color: white !important; padding: 0.55rem 1rem !important; font-weight: 600 !important; }}
          div[data-testid="stDataFrame"] {{ border-radius: 14px; overflow: hidden; }}
          .block-container {{ padding-top: 1.1rem; padding-bottom: 1.2rem; }}
        </style>
        """,
        unsafe_allow_html=True,
    )

def _logo_html(width_px: int = 220, top_pad_px: int = 10) -> str:
    logo_path = Path(__file__).parent / "assets" / "KrispCallLogo.png"
    if not logo_path.exists():
        return ""
    b64 = base64.b64encode(logo_path.read_bytes()).decode("utf-8")
    return f'<div style="padding-top:{top_pad_px}px;"><img src="data:image/png;base64,{b64}" style="width:{width_px}px; height:auto;" /></div>'

def require_login():
    st.session_state.setdefault("authenticated", False)
    if st.session_state["authenticated"]:
        return
    _inject_brand_css()
    c1, c2 = st.columns([1, 2], vertical_alignment="center")
    with c1:
        st.markdown(_logo_html(width_px=260, top_pad_px=14), unsafe_allow_html=True)
    with c2:
        st.markdown('<div class="kc-hero"><h1>Payment Summary</h1><p>Secure login required.</p></div>', unsafe_allow_html=True)
    u = _get_secret(["auth", "username"])
    p = _get_secret(["auth", "password"])
    if not u or not p:
        st.error("Missing auth secrets.")
        st.stop()
    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        if st.form_submit_button("Login"):
            if username == str(u) and password == str(p):
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("Invalid credentials.")
    st.stop()

def _mixpanel_headers() -> Dict[str, str]:
    auth = _get_secret(["mixpanel", "authorization"])
    if not auth:
        raise RuntimeError("Missing mixpanel.authorization in Streamlit secrets.")
    return {"accept": "text/plain", "authorization": str(auth).strip()}

# =========================
# Core utilities
# =========================
def _excel_safe(v):
    if v is None or v is pd.NA: return ""
    if isinstance(v, float) and (np.isnan(v) or np.isinf(v)): return ""
    if isinstance(v, (np.floating, np.integer)): return v.item()
    if isinstance(v, pd.Timestamp):
        if v.tzinfo is not None: v = v.tz_convert(None)
        return v.to_pydatetime()
    if isinstance(v, (datetime, date)): return v
    if isinstance(v, (list, tuple, set)): return ", ".join([str(x) for x in v])
    return v

def _norm_text(val) -> str:
    if val is None: return ""
    return str(val).strip().lower()

def _extract_emails(value) -> List[str]:
    if value is None: return []
    try:
        if isinstance(value, float) and pd.isna(value): return []
    except Exception: pass
    found = EMAIL_REGEX.findall(str(value))
    out: List[str] = []
    seen = set()
    for e in found:
        e2 = e.strip().lower()
        if e2 and e2 not in seen:
            seen.add(e2)
            out.append(e2)
    return out

def _pick_first_existing_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower_map: return lower_map[c.lower()]
    for c in candidates:
        if c in df.columns: return c
    return None

def _parse_time_to_dt(series: pd.Series) -> pd.Series:
    t = pd.to_numeric(series, errors="coerce")
    if t.dropna().empty: return pd.to_datetime(series, errors="coerce", utc=True)
    if float(t.median()) > 1e11: t = (t // 1000)
    return pd.to_datetime(t, unit="s", utc=True)

# =========================
# Mixpanel
# =========================
def dedupe_mixpanel_export(df: pd.DataFrame) -> pd.DataFrame:
    required = ["event", "distinct_id", "time", "$insert_id"]
    missing = [c for c in required if c not in df.columns]
    if missing: raise KeyError(f"Missing columns: {missing}")
    df = df.copy()
    t = pd.to_numeric(df["time"], errors="coerce")
    if t.notna().all():
        if float(t.median()) > 1e11: t = (t // 1000)
        df["_time_s"] = t.astype("Int64")
    else:
        dt = pd.to_datetime(df["time"], errors="coerce", utc=True)
        df["_time_s"] = (dt.view("int64") // 10**9).astype("Int64")
    sort_cols = ["_time_s"]
    if "mp_processing_time_ms" in df.columns: sort_cols = ["mp_processing_time_ms"] + sort_cols
    df = df.sort_values(sort_cols, kind="mergesort")
    return df.drop_duplicates(subset=["event", "distinct_id", "_time_s", "$insert_id"], keep="last").drop(columns=["_time_s"])

@st.cache_data(show_spinner=False, ttl=600)
def fetch_mixpanel_event_export(project_id: int, base_url: str, from_date: date, to_date: date, event_name: str) -> pd.DataFrame:
    url = f"{base_url.rstrip('/')}/api/2.0/export"
    params = {"project_id": int(project_id), "from_date": from_date.isoformat(), "to_date": to_date.isoformat(), "event": json.dumps([event_name])}
    resp = requests.get(url, params=params, headers=_mixpanel_headers(), timeout=180)
    if resp.status_code != 200: raise RuntimeError(f"Mixpanel export failed: {resp.status_code}")
    objs = []
    for line in resp.text.splitlines():
        if line.strip():
            try: objs.append(json.loads(line))
            except: continue
    if not objs: return pd.DataFrame()
    raw = pd.DataFrame(objs)
    if "properties" in raw.columns:
        props = pd.json_normalize(raw["properties"])
        raw = pd.concat([raw.drop(columns=["properties"]), props], axis=1)
    if "time" in raw.columns: raw["_dt"] = _parse_time_to_dt(raw["time"])
    return raw

# =========================
# Logic & Calculation
# =========================
def _split_labels(value) -> List[str]:
    if value is None: return []
    parts = [p.strip() for p in str(value).split(",")]
    return [p for p in parts if p]

def _connected_from_labels(labels: List[str]) -> bool:
    labs = [str(l).strip().lower() for l in (labels or [])]
    return any("connected" in l for l in labs) and not any(l == "not connected" for l in labs)

def _filter_leads_initial(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """Filters out India and Junk Leads."""
    logs = []
    out = df.copy()
    initial_len = len(out)
    
    # Country Filter (Person - Country != India)
    # Check flexible names
    country_col = _pick_first_existing_column(out, ["Person - Country", "Country", "Person Country"])
    if country_col:
        out = out[out[country_col].astype(str).str.strip().str.lower() != "india"]
    
    # Label Filter (Does not contain "Junk Lead")
    label_col = _pick_first_existing_column(out, ["Lead - Label", "Label", "Labels", "Lead - Labels"])
    if label_col:
        # Filter where label contains "junk lead" (case insensitive)
        mask = out[label_col].astype(str).str.contains("junk lead", case=False, na=False)
        out = out[~mask]
    
    filtered_count = initial_len - len(out)
    if filtered_count > 0:
        logs.append(f"Filtered {filtered_count} leads (Country=India or Label='Junk Lead').")
    
    return out, logs

def _expand_leads_for_multiple_emails(df: pd.DataFrame, email_cols_priority: List[str]) -> Tuple[pd.DataFrame, List[int]]:
    missing_rows = []
    expanded = []
    for i, row in df.iterrows():
        emails = []
        for col in email_cols_priority:
            if col in df.columns:
                found = _extract_emails(row[col])
                if found:
                    emails = found
                    break
        if not emails:
            rec = row.to_dict()
            rec["email"] = None
            expanded.append(rec)
            missing_rows.append(i + 2)
            continue
        for e in emails:
            rec = row.to_dict()
            rec["email"] = e
            expanded.append(rec)
    return pd.DataFrame(expanded), missing_rows

def _filter_credit_excluded(df: pd.DataFrame, text_col: Optional[str]) -> pd.DataFrame:
    if df.empty or not text_col or text_col not in df.columns: return df.copy()
    mask = df[text_col].apply(_norm_text).isin(CREDIT_EXCLUDE_DESCS)
    return df[~mask].copy()

def _windowed_email_summary(
    payments_gross: pd.DataFrame, refunds_gross: pd.DataFrame,
    payments_ce: pd.DataFrame, refunds_ce: pd.DataFrame,
    amount_col: str, desc_col: Optional[str], refund_amount_col: str, days: int = 7
) -> pd.DataFrame:
    d = payments_gross.dropna(subset=["email"]).copy()
    if d.empty:
        return pd.DataFrame(columns=["email", "Net_Amount", "Net_Amount_creditExcluded", "Total_Amount", "Total_Amount_creditExcluded", "Refund_Amount", "Refund_Amount_creditExcluded"])
    
    d[amount_col] = pd.to_numeric(d[amount_col], errors="coerce").fillna(0.0)
    ce_map = {e: g.sort_values("_dt") for e, g in payments_ce.dropna(subset=["email"]).groupby("email")} if not payments_ce.empty else {}
    ref_map = {e: g.sort_values("_dt") for e, g in refunds_gross.dropna(subset=["email"]).groupby("email")} if not refunds_gross.empty else {}
    ref_ce_map = {e: g.sort_values("_dt") for e, g in refunds_ce.dropna(subset=["email"]).groupby("email")} if not refunds_ce.empty else {}

    out = []
    for email, g in d.groupby("email", sort=False):
        g = g.sort_values("_dt")
        trigger = False
        start = None
        if desc_col and desc_col in g.columns:
            mask = g[desc_col].astype(str).str.contains("Workspace Subscription", case=False, na=False)
            if mask.any():
                trigger = True
                start = g.loc[mask, "_dt"].min()
        if start is None: start = g["_dt"].min()
        end = start + timedelta(days=days)

        gross_total = float(g[(g["_dt"] >= start) & (g["_dt"] <= end)][amount_col].sum())
        
        g_ce = ce_map.get(email)
        ce_total = float(g_ce[(g_ce["_dt"] >= start) & (g_ce["_dt"] <= end)][amount_col].sum()) if g_ce is not None else 0.0
        
        g_ref = ref_map.get(email)
        ref_total = float(g_ref[(g_ref["_dt"] >= start) & (g_ref["_dt"] <= end)][refund_amount_col].sum()) if g_ref is not None else 0.0
        
        g_ref_ce = ref_ce_map.get(email)
        ref_ce_total = float(g_ref_ce[(g_ref_ce["_dt"] >= start) & (g_ref_ce["_dt"] <= end)][refund_amount_col].sum()) if g_ref_ce is not None else 0.0

        out.append({
            "email": email,
            "Net_Amount": gross_total - ref_total,
            "Net_Amount_creditExcluded": ce_total - ref_ce_total,
            "Total_Amount": gross_total,
            "Total_Amount_creditExcluded": ce_total,
            "Refund_Amount": ref_total,
            "Refund_Amount_creditExcluded": ref_ce_total
        })
    return pd.DataFrame(out)

def _strict_range_email_summary(
    payments_gross: pd.DataFrame, refunds_gross: pd.DataFrame,
    payments_ce: pd.DataFrame, refunds_ce: pd.DataFrame,
    amount_col: str, refund_amount_col: str
) -> pd.DataFrame:
    def get_sums(df, out_col):
        if df.empty: return pd.Series(dtype=float)
        return df.groupby("email")[amount_col if "Total" in out_col else refund_amount_col].sum().rename(out_col)

    p_gross = get_sums(payments_gross, "Period_Total_Amount")
    p_ce = get_sums(payments_ce, "Period_Total_Amount_creditExcluded")
    r_gross = get_sums(refunds_gross, "Period_Refund_Amount")
    r_ce = get_sums(refunds_ce, "Period_Refund_Amount_creditExcluded")

    df = pd.concat([p_gross, p_ce, r_gross, r_ce], axis=1).fillna(0.0)
    df.index.name = "email"
    df = df.reset_index()
    df["Period_Net_Amount"] = df["Period_Total_Amount"] - df["Period_Refund_Amount"]
    df["Period_Net_Amount_creditExcluded"] = df["Period_Total_Amount_creditExcluded"] - df["Period_Refund_Amount_creditExcluded"]
    return df

def _add_totals_row(df: pd.DataFrame, label_col: Optional[str] = None) -> pd.DataFrame:
    if df is None or df.empty: return df
    out = df.copy()
    num_cols = out.select_dtypes(include=[np.number]).columns.tolist()
    totals = {c: float(pd.to_numeric(out[c], errors="coerce").fillna(0).sum()) for c in num_cols}
    row = {c: "" for c in out.columns}
    if label_col and label_col in out.columns: row[label_col] = "TOTAL"
    else: row[out.columns[0]] = "TOTAL"
    row.update(totals)
    return pd.concat([out, pd.DataFrame([row])], ignore_index=True)

def _style_sheet(ws):
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center")
    ws.freeze_panes = "A2"
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
        first = row[0].value
        if first and str(first).strip().upper() == "TOTAL":
            for cell in row: cell.font = Font(bold=True)
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = 20

def _build_excel(
    leads_with_payments: pd.DataFrame,
    leads_nonzero: pd.DataFrame,
    owner_summary: pd.DataFrame,
    owner_x_connected: pd.DataFrame,
    connected_summary: pd.DataFrame,
    label_summary: pd.DataFrame,
    time_summary: pd.DataFrame,
    duplicate_leads: pd.DataFrame,
    self_converted_emails: pd.DataFrame,
    overall_metrics_df: pd.DataFrame,
    logs_df: pd.DataFrame,
    fig_owner, fig_time,
    from_date: date, to_date: date,
) -> Tuple[str, bytes]:
    wb = Workbook()

    def add_sheet(title: str, df: pd.DataFrame, label_col: Optional[str] = None, add_totals: bool = True):
        ws = wb.create_sheet(title)
        df2 = _add_totals_row(df, label_col=label_col) if add_totals and not df.empty else df
        if df2 is None or df2.empty:
            ws.append(["No data"])
        else:
            for r in dataframe_to_rows(df2, index=False, header=True):
                ws.append([_excel_safe(x) for x in r])
        _style_sheet(ws)
        return ws

    ws0 = wb.active
    ws0.title = "Overall Metrics"
    for r in dataframe_to_rows(overall_metrics_df, index=False, header=True):
        ws0.append([_excel_safe(x) for x in r])
    _style_sheet(ws0)

    add_sheet("Leads_with_Payments", leads_with_payments)
    add_sheet("Leads_Payments_NonZero", leads_nonzero)
    add_sheet("Owner_Summary", owner_summary, label_col=owner_summary.columns[0] if not owner_summary.empty else None, add_totals=False)
    add_sheet("Connected_Summary", connected_summary, label_col="Connected")
    add_sheet("Owner_x_Connected", owner_x_connected, label_col=owner_x_connected.columns[0] if not owner_x_connected.empty else None)
    add_sheet("Label_Summary", label_summary, label_col="Label")
    add_sheet("Time_Summary", time_summary, label_col=time_summary.columns[0] if not time_summary.empty else None)
    add_sheet("Duplicate_Leads", duplicate_leads, label_col="email")
    add_sheet("SelfConverted_Emails", self_converted_emails, label_col="email")
    add_sheet("Logs", logs_df)

    ws_chart = wb.create_sheet("Charts")
    img = XLImage(BytesIO())
    fig_owner.savefig(img.fp, format="png")
    img.anchor = "A1"
    ws_chart.add_image(img)
    
    img2 = XLImage(BytesIO())
    fig_time.savefig(img2.fp, format="png")
    img2.anchor = "A25"
    ws_chart.add_image(img2)
    
    if "Sheet" in wb.sheetnames: del wb["Sheet"]
    out = BytesIO()
    wb.save(out)
    return f"payment_summary_{from_date}_{to_date}.xlsx", out.getvalue()

# =========================
# Main app
# =========================
def main():
    st.set_page_config(page_title="KrispCall Payment Summary", page_icon="📈", layout="wide")
    require_login()
    _inject_brand_css()

    st.markdown('<div style="height:10px;"></div>', unsafe_allow_html=True)
    c1, c2 = st.columns([1, 3], vertical_alignment="center")
    with c1: st.markdown(_logo_html(width_px=240, top_pad_px=14), unsafe_allow_html=True)
    with c2: st.markdown('<div class="kc-hero"><h1>KrispCall Payment Summary</h1></div>', unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("### Date Selection")
        today = date.today()
        from_date = st.date_input("Date from", value=today.replace(day=1) - timedelta(days=4))
        to_date = st.date_input("Date to", value=today - timedelta(days=1))
        st.markdown("### Rules")
        st.info("Leads with 'India' or 'Junk Lead' are filtered out.")
        st.info("Overall Conversions: Emails with 'Workspace Subscription'.")
        st.info("Self Converted: Overall Conversions - Sales Attempted Conversions.")
        st.info("Sales Attempted: Subscribers present in filtered leads (Excl Pipedrive KC).")

    st.markdown('<div class="kc-card">', unsafe_allow_html=True)
    leads_file = st.file_uploader("Upload Leads CSV", type=["csv"])
    run = st.button("Run Analysis", type="primary", disabled=(leads_file is None))
    st.markdown("</div>", unsafe_allow_html=True)

    if not run: st.stop()

    logs = []
    with st.spinner("Processing..."):
        leads_raw = pd.read_csv(leads_file)
        
        # 1. Global Filter (India / Junk Lead)
        leads_filtered_initial, filter_logs = _filter_leads_initial(leads_raw)
        logs.extend(filter_logs)
        
        # 2. Identify Columns
        owner_col = _pick_first_existing_column(leads_filtered_initial, ["Lead - Owner", "Deal - Owner", "Owner", "owner"]) or "Owner"
        created_col = _pick_first_existing_column(leads_filtered_initial, ["Lead - Lead created on", "Lead created on", "Created on"])
        label_col = _pick_first_existing_column(leads_filtered_initial, ["Lead - Label", "Label", "Labels", "Lead - Labels"])
        
        email_cols_priority = []
        for cand in ["Person - Email", "Lead - User Email"]:
            col = _pick_first_existing_column(leads_filtered_initial, [cand])
            if col and col not in email_cols_priority: email_cols_priority.append(col)
        if not email_cols_priority:
            email_cols_priority = [c for c in leads_filtered_initial.columns if "email" in c.lower()][:5]

        # 3. Expand Emails
        expanded_leads, missing_rows = _expand_leads_for_multiple_emails(leads_filtered_initial, email_cols_priority)
        if missing_rows: logs.append(f"Rows with no email extracted: {len(missing_rows)}")

        # 4. Enriched Attributes
        if label_col and label_col in expanded_leads.columns:
            expanded_leads["labels_list"] = expanded_leads[label_col].apply(_split_labels)
        else:
            expanded_leads["labels_list"] = [[] for _ in range(len(expanded_leads))]
        expanded_leads["Connected"] = expanded_leads["labels_list"].apply(_connected_from_labels)
        expanded_leads["_lead_created_dt"] = pd.to_datetime(expanded_leads[created_col], errors="coerce") if created_col else pd.NaT

        # 5. Fetch Mixpanel
        pid = int(_get_secret(["mixpanel", "project_id"]))
        base = _get_secret(["mixpanel", "base_url"], "https://data-eu.mixpanel.com")
        
        payments_raw = fetch_mixpanel_event_export(pid, base, from_date, to_date, "New Payment Made")
        refunds_raw = fetch_mixpanel_event_export(pid, base, from_date, to_date, "Refund Granted")

        payments = dedupe_mixpanel_export(payments_raw) if not payments_raw.empty else payments_raw
        refunds = dedupe_mixpanel_export(refunds_raw) if not refunds_raw.empty else refunds_raw
        
        # Mixpanel Parsing
        pay_email_col = _pick_first_existing_column(payments, ["$email", "email", "Email", "user.email"])
        ref_email_col = _pick_first_existing_column(refunds, ["user.email", "$email", "email"])
        
        payments["email"] = payments[pay_email_col].apply(lambda v: (_extract_emails(v)[0] if _extract_emails(v) else None)) if pay_email_col else None
        refunds["email"] = refunds[ref_email_col].apply(lambda v: (_extract_emails(v)[0] if _extract_emails(v) else None)) if ref_email_col else None
        
        amount_col = _pick_first_existing_column(payments, ["Amount", "amount", "Amount Paid"])
        desc_col = _pick_first_existing_column(payments, ["Amount Description", "description"])
        refund_amount_col = _pick_first_existing_column(refunds, ["Refund Amount", "amount"])
        refund_desc_col = _pick_first_existing_column(refunds, ["Refunded Transaction description"])
        
        if not amount_col: raise RuntimeError("Payment Amount column missing.")
        if not refund_amount_col: 
            refunds["Refund Amount"] = 0.0
            refund_amount_col = "Refund Amount"
            
        payments[amount_col] = pd.to_numeric(payments[amount_col], errors="coerce").fillna(0.0)
        refunds[refund_amount_col] = pd.to_numeric(refunds[refund_amount_col], errors="coerce").fillna(0.0)
        
        payments_all_ce = _filter_credit_excluded(payments, desc_col)
        refunds_all_ce = _filter_credit_excluded(refunds, refund_desc_col)

        # ---------------------------
        # GLOBAL METRICS LOGIC
        # ---------------------------
        # 1. Overall Conversions (Subscribers)
        sub_mask = pd.Series(False, index=payments.index)
        if desc_col:
            sub_mask = payments[desc_col].astype(str).str.contains("Workspace Subscription", case=False, na=False)
        overall_conversions_emails = set(payments.loc[sub_mask, "email"].dropna().unique())
        
        # 2. Sales Attempted Base (Filtered Leads excluding Pipedrive KC)
        # Note: expanded_leads is already filtered for India/Junk. Now filter Owner.
        sales_attempted_base_rows = expanded_leads[~expanded_leads[owner_col].astype(str).str.strip().str.lower().eq(EXCLUDED_OWNER_CANON)].copy()
        sales_attempted_leads_emails = set(sales_attempted_base_rows["email"].dropna().unique())
        
        # 3. Sales Effort Base (Connected)
        sales_effort_base_rows = sales_attempted_base_rows[sales_attempted_base_rows["Connected"] == True].copy()
        sales_effort_leads_emails = set(sales_effort_base_rows["email"].dropna().unique())

        # 4. Hierarchical Conversion Classification
        # Sales Attempted Conversions = Intersection(Subscribers, Leads_No_KC)
        sales_attempted_conversion_emails = overall_conversions_emails.intersection(sales_attempted_leads_emails)
        
        # Self Converted Conversions = Overall - Sales Attempted (As per equation request)
        self_converted_conversion_emails = overall_conversions_emails - sales_attempted_conversion_emails
        
        # Sales Effort Conversions = Subset of Sales Attempted where Connected
        sales_effort_conversion_emails = sales_attempted_conversion_emails.intersection(sales_effort_leads_emails)

        # 5. Revenue Calculation Helper
        def calc_revenue_stats(target_emails: set):
            p_pay = payments[payments["email"].isin(target_emails)]
            p_ref = refunds[refunds["email"].isin(target_emails)]
            
            # Full Duration (Strict)
            rev_full = p_pay[amount_col].sum() - p_ref[refund_amount_col].sum()
            
            # 7-Day Window (Recalculate window for these specific emails)
            p_pay_ce = payments_all_ce[payments_all_ce["email"].isin(target_emails)]
            p_ref_ce = refunds_all_ce[refunds_all_ce["email"].isin(target_emails)]
            
            summ = _windowed_email_summary(
                p_pay, p_ref, p_pay_ce, p_ref_ce, amount_col, desc_col, refund_amount_col, 7
            )
            rev_7d = summ["Net_Amount"].sum() if not summ.empty else 0.0
            return rev_full, rev_7d

        sc_rev_full, sc_rev_7d = calc_revenue_stats(self_converted_conversion_emails)
        sa_rev_full, sa_rev_7d = calc_revenue_stats(sales_attempted_conversion_emails)
        se_rev_full, se_rev_7d = calc_revenue_stats(sales_effort_conversion_emails)

        # 6. Lead Counts
        total_leads_generated = len(expanded_leads) # Already filtered India/Junk
        total_leads_attempted = len(sales_attempted_base_rows)
        total_leads_connected = len(sales_effort_base_rows)

        metrics_data = [
            {"Metric": "Overall Conversions", "Value": len(overall_conversions_emails), "Description": "Unique emails with 'Workspace Subscription'"},
            {"Metric": "Self Converted Conversions", "Value": len(self_converted_conversion_emails), "Description": "Overall - Sales Attempted Conversions"},
            {"Metric": "Sales Attempted Total Conversions", "Value": len(sales_attempted_conversion_emails), "Description": "Subscribers found in Leads (Excl Pipedrive KC)"},
            {"Metric": "Sales Effort Total Conversions", "Value": len(sales_effort_conversion_emails), "Description": "Subset of Sales Attempted where Connected=TRUE"},
            {"Metric": "---", "Value": "---", "Description": "---"},
            {"Metric": "Self Converted - Full Duration", "Value": sc_rev_full, "Description": "Net Revenue from Self Converted (Strict Range)"},
            {"Metric": "Sales Revenue Total - Full Duration", "Value": sa_rev_full, "Description": "Net Revenue from Sales Attempted (Strict Range)"},
            {"Metric": "Sales Effort Total - Full Duration", "Value": se_rev_full, "Description": "Net Revenue from Sales Effort (Strict Range)"},
            {"Metric": "---", "Value": "---", "Description": "---"},
            {"Metric": "Self Converted Revenue (7 day)", "Value": sc_rev_7d, "Description": "Net Revenue (7-Day Window)"},
            {"Metric": "Sales Revenue (7 days)", "Value": sa_rev_7d, "Description": "Net Revenue (7-Day Window)"},
            {"Metric": "Sales Effort Total (7 day)", "Value": se_rev_7d, "Description": "Net Revenue (7-Day Window)"},
            {"Metric": "---", "Value": "---", "Description": "---"},
            {"Metric": "Total Leads Generated", "Value": total_leads_generated, "Description": "Filtered (No India, No Junk Lead)"},
            {"Metric": "Total Leads Attempted", "Value": total_leads_attempted, "Description": "Generated Leads excluding Pipedrive KC Owner"},
            {"Metric": "Total Leads Connected", "Value": total_leads_connected, "Description": "Attempted Leads with Connected=TRUE"},
        ]
        overall_metrics_df = pd.DataFrame(metrics_data)

        # ---------------------------
        # SUMMARIES (Tables)
        # ---------------------------
        # We need to run the summary logic on ALL leads to build the tables
        # Filter: Exclude Pipedrive KC from tables as requested previously
        table_leads_rows = sales_attempted_base_rows.copy() 
        table_leads_emails = set(table_leads_rows["email"].dropna().unique())
        
        # Payment sets for table calculation
        pay_table = payments[payments["email"].isin(table_leads_emails)]
        ref_table = refunds[refunds["email"].isin(table_leads_emails)]
        pay_table_ce = payments_all_ce[payments_all_ce["email"].isin(table_leads_emails)]
        ref_table_ce = refunds_all_ce[refunds_all_ce["email"].isin(table_leads_emails)]

        # Run both calculations
        summ_7d = _windowed_email_summary(pay_table, ref_table, pay_table_ce, ref_table_ce, amount_col, desc_col, refund_amount_col, 7)
        summ_full = _strict_range_email_summary(pay_table, ref_table, pay_table_ce, ref_table_ce, amount_col, refund_amount_col)
        
        joined = table_leads_rows.merge(summ_7d, on="email", how="left")
        joined = joined.merge(summ_full, on="email", how="left")
        
        # Fill numeric NaNs
        num_cols = ["Net_Amount", "Period_Net_Amount", "Total_Amount", "Period_Total_Amount", "Refund_Amount", "Period_Refund_Amount"]
        for c in num_cols: joined[c] = pd.to_numeric(joined[c], errors="coerce").fillna(0.0)

        # Deduplicate joined for Revenue Sums (One row per email)
        joined_dedup = joined.sort_values(["email", "_lead_created_dt"], kind="mergesort").drop_duplicates(subset=["email"], keep="first").copy()

        def build_summary_table(group_cols: List[str], base_df: pd.DataFrame, dedup_df: pd.DataFrame):
            # 1. Lead Count (from base rows)
            counts = base_df.groupby(group_cols, as_index=False).size().rename(columns={"size": "Lead_Count"})
            # 2. Revenue (from dedup rows)
            rev = dedup_df.groupby(group_cols, as_index=False)[["Period_Net_Amount", "Net_Amount"]].sum()
            rev.columns = group_cols + ["Sales Revenue Total - Full Duration", "Sales Revenue (7 days)"]
            # 3. Paying Users (from dedup rows where Revenue > 0)
            # Use 'Overall Conversions' definition? Or just any revenue? 
            # Usually tables imply any revenue, but let's stick to "Conversions" subset if strict.
            # However, prompt says "Sales Attempted Total Conversions" is the count.
            # Let's count unique emails with > 0 revenue in strict period or 7d.
            has_rev = dedup_df[(dedup_df["Period_Total_Amount"] > 0) | (dedup_df["Total_Amount"] > 0)]
            payers = has_rev.groupby(group_cols, as_index=False)["email"].nunique().rename(columns={"email": "Paying Users"})
            
            final = counts.merge(rev, on=group_cols, how="left").merge(payers, on=group_cols, how="left").fillna(0)
            if "Sales Revenue Total - Full Duration" in final.columns:
                final = final.sort_values("Sales Revenue Total - Full Duration", ascending=False)
            return final

        owner_summary = build_summary_table([owner_col], table_leads_rows, joined_dedup)
        connected_summary = build_summary_table(["Connected"], table_leads_rows, joined_dedup)
        owner_x_connected = build_summary_table([owner_col, "Connected"], table_leads_rows, joined_dedup)
        
        # Time Summary
        table_leads_rows["Lead_Created_Date"] = table_leads_rows["_lead_created_dt"].dt.date
        joined_dedup["Lead_Created_Date"] = joined_dedup["_lead_created_dt"].dt.date
        time_summary = build_summary_table(["Lead_Created_Date"], table_leads_rows, joined_dedup).sort_values("Lead_Created_Date")

        # Label Summary (Explode)
        lbl_base = table_leads_rows.explode("labels_list").rename(columns={"labels_list": "Label"})
        lbl_base = lbl_base[lbl_base["Label"].fillna("").astype(str).str.strip() != ""]
        lbl_dedup = joined_dedup.explode("labels_list").rename(columns={"labels_list": "Label"})
        lbl_dedup = lbl_dedup[lbl_dedup["Label"].fillna("").astype(str).str.strip() != ""]
        label_summary = build_summary_table(["Label"], lbl_base, lbl_dedup)

        # Self Converted Detail Table
        pay_sc = payments[payments["email"].isin(self_converted_conversion_emails)]
        ref_sc = refunds[refunds["email"].isin(self_converted_conversion_emails)]
        sc_summ_7d = _windowed_email_summary(pay_sc, ref_sc, payments_all_ce, refunds_all_ce, amount_col, desc_col, refund_amount_col, 7)
        sc_summ_full = _strict_range_email_summary(pay_sc, ref_sc, payments_all_ce, refunds_all_ce, amount_col, refund_amount_col)
        self_converted_fact = sc_summ_7d.merge(sc_summ_full, on="email", how="left").fillna(0)
        self_converted_fact = self_converted_fact[["email", "Period_Net_Amount", "Net_Amount"]].rename(
            columns={"Period_Net_Amount": "Self Converted - Full Duration", "Net_Amount": "Self Converted (7 day)"}
        ).sort_values("Self Converted - Full Duration", ascending=False)

        # Duplicates
        dup_mask = table_leads_rows["email"].notna() & table_leads_rows["email"].duplicated(keep=False)
        duplicate_leads = table_leads_rows[dup_mask].sort_values("email")

        # Charts
        fig_owner, ax = plt.subplots(figsize=(10,6))
        if not owner_summary.empty:
            owner_summary.head(15).set_index(owner_col)[["Sales Revenue Total - Full Duration", "Sales Revenue (7 days)"]].plot(kind="bar", ax=ax)
            ax.set_title("Revenue by Owner")
            plt.tight_layout()
        
        fig_time, ax2 = plt.subplots(figsize=(10,5))
        if not time_summary.empty:
            ax2.plot(pd.to_datetime(time_summary["Lead_Created_Date"]), time_summary["Sales Revenue Total - Full Duration"], label="Full Duration")
            ax2.plot(pd.to_datetime(time_summary["Lead_Created_Date"]), time_summary["Sales Revenue (7 days)"], label="7-Day")
            ax2.legend()
            ax2.set_title("Revenue by Date")
            plt.tight_layout()

        # Excel
        # Prepare exports
        joined_export = joined.drop(columns=["_lead_created_dt", "labels_list"], errors="ignore")
        joined_nonzero = joined[(joined["Period_Total_Amount"]>0) | (joined["Total_Amount"]>0)].copy()
        
        logs_df = pd.DataFrame({"log": logs})

        excel_name, excel_bytes = _build_excel(
            leads_with_payments=joined_export,
            leads_nonzero=joined_nonzero,
            owner_summary=owner_summary,
            owner_x_connected=owner_x_connected,
            connected_summary=connected_summary,
            label_summary=label_summary,
            time_summary=time_summary,
            duplicate_leads=duplicate_leads,
            self_converted_emails=self_converted_fact,
            overall_metrics_df=overall_metrics_df,
            logs_df=logs_df,
            fig_owner=fig_owner, fig_time=fig_time,
            from_date=from_date, to_date=to_date
        )

    # =========================
    # UI Render
    # =========================
    t_overall, t_summ, t_tables, t_chart, t_exp, t_log = st.tabs(["Overall Metrics", "Summaries", "Data Tables", "Charts", "Export", "Logs"])

    with t_overall:
        st.dataframe(overall_metrics_df, use_container_width=True)
        c1, c2 = st.columns(2)
        c1.metric("Overall Conversions", len(overall_conversions_emails))
        c2.metric("Total Leads Attempted", total_leads_attempted)

    with t_summ:
        st.markdown("#### Owner Summary")
        st.dataframe(_style_totals_row(_add_totals_row(owner_summary, label_col=owner_col)), use_container_width=True)
        st.markdown("#### Connected Summary")
        st.dataframe(_style_totals_row(_add_totals_row(connected_summary, label_col="Connected")), use_container_width=True)
        st.markdown("#### Self Converted Detail")
        st.dataframe(self_converted_fact, use_container_width=True)

    with t_tables:
        st.dataframe(joined_export.head(100), use_container_width=True)
        st.caption("Showing first 100 rows of joined data.")

    with t_chart:
        st.pyplot(fig_owner)
        st.pyplot(fig_time)

    with t_exp:
        st.download_button("Download Excel Report", excel_bytes, excel_name)

    with t_log:
        for l in logs: st.info(l)

if __name__ == "__main__":
    main()
