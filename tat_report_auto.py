"""
Valuecart Seller Queries TAT Report — Automated Weekly Script
Runs every Thursday via GitHub Actions.
Fetches tickets from Zoho Desk API, builds Excel report, sends via Gmail.
"""

import os
import io
import json
import base64
import requests
import pandas as pd
from datetime import datetime, timedelta
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.worksheet.formula import ArrayFormula
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import smtplib
from email.utils import formataddr


# ══════════════════════════════════════════════════════════════
#  CONFIG  — set these as GitHub Actions Secrets
# ══════════════════════════════════════════════════════════════
ZOHO_CLIENT_ID       = os.environ['ZOHO_CLIENT_ID']
ZOHO_CLIENT_SECRET   = os.environ['ZOHO_CLIENT_SECRET']
ZOHO_REFRESH_TOKEN   = os.environ['ZOHO_REFRESH_TOKEN']
ZOHO_ORG_ID          = os.environ['ZOHO_ORG_ID']          # Zoho Desk org ID

GMAIL_ADDRESS        = os.environ['GMAIL_ADDRESS']   # your gmail address
GMAIL_APP_PASSWORD   = os.environ['GMAIL_APP_PASSWORD'] # Gmail App Password

EMAIL_TO             = os.environ['EMAIL_TO']             # recipient
EMAIL_CC             = os.environ.get('EMAIL_CC', '')     # optional CC


# ══════════════════════════════════════════════════════════════
#  STEP 1 — Compute Week Ranges (Sun–Sat, current week = today)
# ══════════════════════════════════════════════════════════════
def get_week_range(date):
    """Return (sunday, saturday) of the week containing date (Sun=start)."""
    days_since_sunday = (date.weekday() + 1) % 7
    sunday = date - timedelta(days=days_since_sunday)
    saturday = sunday + timedelta(days=6)
    return sunday.replace(hour=0, minute=0, second=0, microsecond=0), \
           saturday.replace(hour=23, minute=59, second=59, microsecond=999999)

today   = datetime.now()
# Current week = this week (in progress)
# Week B (recent)  = last week
# Week A (older)   = 2 weeks ago
curr_sun, curr_sat = get_week_range(today)
wB_start = curr_sun - timedelta(weeks=1)
wB_end   = curr_sun - timedelta(seconds=1)
wA_start = curr_sun - timedelta(weeks=2)
wA_end   = curr_sun - timedelta(weeks=1, seconds=1)

# ISO week numbers for labels
wA_num = wA_start.isocalendar()[1]
wB_num = wB_start.isocalendar()[1]
WK_A_LABEL = f"Week {wA_num}"
WK_B_LABEL = f"Week {wB_num}"

print(f"Generating report for {WK_B_LABEL} ({wB_start.date()} – {wB_end.date()}) "
      f"vs {WK_A_LABEL} ({wA_start.date()} – {wA_end.date()})")


# ══════════════════════════════════════════════════════════════
#  STEP 2 — Fetch from Zoho Desk API
# ══════════════════════════════════════════════════════════════
def get_zoho_access_token():
    r = requests.post('https://accounts.zoho.in/oauth/v2/token', params={
        'refresh_token': ZOHO_REFRESH_TOKEN,
        'client_id':     ZOHO_CLIENT_ID,
        'client_secret': ZOHO_CLIENT_SECRET,
        'grant_type':    'refresh_token',
    })
    resp = r.json()
    if 'access_token' not in resp:
        raise Exception(f"Zoho token error: {resp}")
    return resp['access_token']


def fetch_all_tickets(access_token):
    """Fetch all recent tickets from Zoho Desk."""
    headers = {
        'Authorization': f'Zoho-oauthtoken {access_token}',
        'orgId': ZOHO_ORG_ID,
    }
    tickets = []
    limit  = 100
    offset = 0
    while True:
        r = requests.get('https://desk.zoho.in/api/v1/tickets', headers=headers, params={
            'limit':     limit,
            'from':      offset,
            'sortBy':    'createdTime',
            'order':     'desc',
        })
        print(f"  API response status: {r.status_code}")
        if r.status_code != 200:
            print(f"  API error: {r.text[:500]}")
            break
        data = r.json().get('data', [])
        if not data:
            break
        tickets.extend(data)
        # Stop if oldest ticket in page is before wA_start
        last_created = data[-1].get('createdTime', '')
        if last_created and last_created < wA_start.strftime('%Y-%m-%dT%H:%M:%S'):
            break
        if len(data) < limit:
            break
        offset += limit
        if offset > 500:  # safety limit
            break
    print(f"  Total tickets fetched: {len(tickets)}")
    return tickets

def fetch_zoho_tickets(access_token, from_date, to_date):
    """Filter tickets by date range."""
    all_tickets = fetch_all_tickets(access_token)
    from_str = from_date.strftime('%Y-%m-%dT%H:%M:%S')
    to_str   = to_date.strftime('%Y-%m-%dT%H:%M:%S')
    filtered = [t for t in all_tickets
                if from_str <= (t.get('createdTime','') or '')[:19] <= to_str]
    print(f"  Filtered {len(filtered)} tickets between {from_str} and {to_str}")
    return filtered


def tickets_to_df(tickets):
    if not tickets:
        return pd.DataFrame(columns=['SLA Violation Type','Subject','Status',
            'Ticket Closed Time','Request Id','Due Date','BD POC',
            'Query Category','Seller Category','Internal Department'])
    rows = []
    for t in tickets:
        cf = t.get('cf') or {}
        # SLA violation type — check multiple possible field names
        sla = (t.get('slaViolationType') or
               t.get('sla_violation_type') or
               cf.get('cf_sla_violation_type') or '')
        # Normalize common values
        if sla in ('', None): sla = 'Not Violated'

        rows.append({
            'SLA Violation Type':  sla,
            'Subject':             t.get('subject', ''),
            'Status':              t.get('status', ''),
            'Ticket Closed Time':  t.get('closedTime', '') or t.get('closed_time', ''),
            'Request Id':          t.get('ticketNumber', '') or t.get('ticket_number', ''),
            'Due Date':            t.get('dueDate', '') or t.get('due_date', ''),
            'BD POC':              (t.get('assignee') or {}).get('name', ''),
            'Query Category':      (cf.get('cf_query_category') or
                                    cf.get('Query Category') or
                                    t.get('category', '') or ''),
            'Seller Category':     (cf.get('cf_seller_category') or
                                    cf.get('Seller Category') or ''),
            'Internal Department': (cf.get('cf_internal_department') or
                                    cf.get('Internal Department') or ''),
        })
    return pd.DataFrame(rows)


print("Fetching Zoho tickets...")
token    = get_zoho_access_token()
tix_A    = fetch_zoho_tickets(token, wA_start, wA_end)
tix_B    = fetch_zoho_tickets(token, wB_start, wB_end)

wk_A_df  = tickets_to_df(tix_A)
wk_B_df  = tickets_to_df(tix_B)

print(f"  {WK_A_LABEL}: {len(wk_A_df)} tickets")
print(f"  {WK_B_LABEL}: {len(wk_B_df)} tickets")

# Excluding On Hold
excl_df  = pd.concat([wk_A_df, wk_B_df], ignore_index=True)
excl_df  = excl_df[excl_df['Status'] != 'On Hold'].reset_index(drop=True)
resv_df  = excl_df[excl_df['SLA Violation Type'] == 'Resolution Violation'].reset_index(drop=True)

print(f"  Excluding On Hold: {len(excl_df)} tickets")
print(f"  Resolution Violation: {len(resv_df)} tickets")


# ══════════════════════════════════════════════════════════════
#  STEP 3 — Build Excel
# ══════════════════════════════════════════════════════════════
PURPLE = 'FF7030A0'
LAVEND = 'FFF2E6FF'

def sc(cell, value=None, bold=False, bg=None, fc=None,
       ha='general', sz=11, fmt=None):
    if value is not None:
        cell.value = value
    cell.font      = Font(bold=bold, color=fc or 'FF000000', size=sz, name='Calibri')
    cell.alignment = Alignment(horizontal=ha, vertical='center')
    if bg:
        cell.fill  = PatternFill('solid', fgColor=bg)
    if fmt:
        cell.number_format = fmt

def write_data_sheet(wb, name, df):
    ws = wb.create_sheet(name)
    COL_WIDTHS = {'A':17.7,'B':59.2,'C':7.6,'D':16.7,
                  'E':9.9,'F':15.4,'G':11.8,'H':21.4,'I':13.7,'J':18.3}
    for c_idx, col in enumerate(df.columns, 1):
        cell = ws.cell(1, c_idx, col)
        sc(cell, col, bold=True, ha='left' if c_idx == 1 else 'center')
    for r_idx, (_, row) in enumerate(df.iterrows(), start=2):
        for c_idx, col in enumerate(df.columns, 1):
            val = row[col]
            sc(ws.cell(r_idx, c_idx, None if pd.isna(val) else val),
               None if pd.isna(val) else val,
               ha='left' if c_idx == 1 else 'center')
    for col_letter, width in COL_WIDTHS.items():
        ws.column_dimensions[col_letter].width = width
    return ws

wb = Workbook()
wb.remove(wb.active)

write_data_sheet(wb, f'WK_{wA_num}', wk_A_df)
write_data_sheet(wb, f'WK_{wB_num}', wk_B_df)
write_data_sheet(wb, 'Excluding On Hold', excl_df)
write_data_sheet(wb, 'Resolution Violation', resv_df)

# Report sheet
ws = wb.create_sheet('Report')
ws.column_dimensions['A'].width = 40.8
ws.column_dimensions['B'].width = 22.2
ws.column_dimensions['C'].width = 13.0
ws.column_dimensions['D'].width = 27.8
ws.column_dimensions['E'].width = 14.8
ws.row_dimensions[1].height = 28

WK_A_SHEET = f'WK_{wA_num}'
WK_B_SHEET = f'WK_{wB_num}'
EXC        = "'Excluding On Hold'"

# Title
ws.merge_cells('A1:D1')
sc(ws['A1'], 'Valuecart Seller Queries TAT Report',
   bold=True, bg=PURPLE, fc='FFFFFFFF', ha='center', sz=14)
for c in ['B1','C1','D1']:
    ws[c].fill = PatternFill('solid', fgColor=PURPLE)

# KPI header
for coord, val, ha in [('A3','KPI','left'),('B3',WK_B_LABEL,'center'),
                        ('C3',WK_A_LABEL,'center'),('D3','Trend','center')]:
    sc(ws[coord], val, bold=True, bg=PURPLE, fc='FFFFFFFF', ha=ha)

# KPI rows
kpi_rows = [
    (4, 'Total Queries Received',
         f'=COUNTIFS(\'{WK_B_SHEET}\'!C2:C1000,"<>",\'{WK_B_SHEET}\'!C2:C1000,"<>On Hold")',
         f'=COUNTIFS(\'{WK_A_SHEET}\'!C2:C1000,"<>",\'{WK_A_SHEET}\'!C2:C1000,"<>On Hold")',
         '=IF(B4>C4,"⬆",IF(B4<C4,"⬇","➡"))', None),
    (5, 'Queries Closed',
         f'=COUNTIFS(\'{WK_B_SHEET}\'!C2:C1000,"Closed")',
         f'=COUNTIFS(\'{WK_A_SHEET}\'!C2:C1000,"Closed")',
         '=IF(B5>C5,"⬆",IF(B5<C5,"⬇","➡"))', None),
    (6, 'Closure Within TAT (%)',
         f'=IFERROR(COUNTIFS(\'{WK_B_SHEET}\'!A2:A1000,"Not Violated",\'{WK_B_SHEET}\'!C2:C1000,"Closed")/COUNTIF(\'{WK_B_SHEET}\'!C2:C1000,"Closed"),0)',
         f'=IFERROR(COUNTIFS(\'{WK_A_SHEET}\'!A2:A1000,"Not Violated",\'{WK_A_SHEET}\'!C2:C1000,"Closed")/COUNTIF(\'{WK_A_SHEET}\'!C2:C1000,"Closed"),0)',
         '=IF(B6>C6,"⬆",IF(B6<C6,"⬇","➡"))', LAVEND),
    (7, 'Open Queries',
         f'=COUNTIFS(\'{WK_B_SHEET}\'!C2:C1000,"Open")',
         f'=COUNTIFS(\'{WK_A_SHEET}\'!C2:C1000,"Open")',
         '=IF(B7>C7,"⬆",IF(B7<C7,"⬇","➡"))', None),
]

for row, label, fB, fA, ftrend, bg in kpi_rows:
    ws.row_dimensions[row].height = 18
    sc(ws[f'A{row}'], label, bold=True, ha='left', bg=bg or 'FFFFFFFF')
    sc(ws[f'B{row}'], fB, ha='center', bg=bg or 'FFFFFFFF', fmt='0%' if row==6 else 'General')
    sc(ws[f'C{row}'], fA, ha='center', bg=bg or 'FFFFFFFF', fmt='0%' if row==6 else 'General')
    sc(ws[f'D{row}'], ftrend, sz=14, ha='center', bg=bg or 'FFFFFFFF')

# Category header
for coord, val, ha in [('A9','Query Category','left'),('B9','Received','center'),
                        ('C9','Closed','center'),('D9','Closed Within TAT','center'),
                        ('E9','TAT %','center')]:
    sc(ws[coord], val, bold=True, bg=PURPLE, fc='FFFFFFFF', ha=ha)

# Dynamic categories from data only
CATEGORIES = sorted(excl_df['Query Category'].dropna().unique().tolist())
if excl_df['Query Category'].isna().any():
    CATEGORIES.append('(Blank)')

for i, cat in enumerate(CATEGORIES):
    row = 10 + i
    ws.row_dimensions[row].height = 18
    sc(ws[f'A{row}'], cat, bold=True, ha='left')
    if cat == '(Blank)':
        ws[f'B{row}'].value     = ArrayFormula(f'B{row}', f'=SUMPRODUCT(({EXC}!H2:H1000="")*({EXC}!H2:H1000<>FALSE)*1)')
        ws[f'B{row}'].font      = Font(size=11, name='Calibri')
        ws[f'B{row}'].alignment = Alignment(horizontal='center', vertical='center')
        sc(ws[f'C{row}'], f'=COUNTIFS({EXC}!C2:C1000,"Closed",{EXC}!H2:H1000,"")', ha='center')
        sc(ws[f'D{row}'], f'=COUNTIFS({EXC}!C2:C1000,"Closed",{EXC}!H2:H1000,"",{EXC}!A2:A1000,"Not Violated")', ha='center')
    else:
        sc(ws[f'B{row}'], f'=COUNTIF({EXC}!H2:H1000,"{cat}")', ha='center')
        sc(ws[f'C{row}'], f'=COUNTIFS({EXC}!H2:H1000,"{cat}",{EXC}!C2:C1000,"Closed")', ha='center')
        sc(ws[f'D{row}'], f'=COUNTIFS({EXC}!H2:H1000,"{cat}",{EXC}!C2:C1000,"Closed",{EXC}!A2:A1000,"Not Violated")', ha='center')
    sc(ws[f'E{row}'], f'=IFERROR(D{row}/C{row},0)', bold=True, ha='center', fmt='0%')

# Save to buffer
excel_buffer = io.BytesIO()
wb.save(excel_buffer)
excel_buffer.seek(0)
excel_bytes = excel_buffer.read()
print("Excel report built.")


# ══════════════════════════════════════════════════════════════
#  STEP 4 — Send via Gmail API
# ══════════════════════════════════════════════════════════════
# SMTP sending — no OAuth needed


today_str    = datetime.now().strftime('%d %b %Y')
filename     = f"TAT_Report_{WK_B_LABEL.replace(' ','_')}_{WK_A_LABEL.replace(' ','_')}_{datetime.now().strftime('%Y%m%d')}.xlsx"
subject      = f"Valuecart Seller Queries TAT Report – {WK_B_LABEL} & {WK_A_LABEL} ({today_str})"
body_text    = f"""Hi Team,

Please find the Valuecart Seller Queries TAT Report for the last two completed weeks.

{WK_B_LABEL}: {wB_start.strftime('%d %b')} – {wB_end.strftime('%d %b %Y')}
{WK_A_LABEL}: {wA_start.strftime('%d %b')} – {wA_end.strftime('%d %b %Y')}

Regards"""

msg = MIMEMultipart()
msg['from']    = GMAIL_ADDRESS
msg['to']      = EMAIL_TO   # supports "a@x.com,b@x.com"
msg['subject'] = subject
if EMAIL_CC:
    msg['cc'] = EMAIL_CC
msg.attach(MIMEText(body_text, 'plain'))

# Build full recipient list
all_recipients = [e.strip() for e in EMAIL_TO.split(',')]
if EMAIL_CC:
    all_recipients += [e.strip() for e in EMAIL_CC.split(',')]

attachment = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
attachment.set_payload(excel_bytes)
encoders.encode_base64(attachment)
attachment.add_header('Content-Disposition', 'attachment', filename=filename)
msg.attach(attachment)

# Send via SMTP
with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
    server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
    server.sendmail(GMAIL_ADDRESS, all_recipients, msg.as_bytes())
print(f"Email sent to: {', '.join(all_recipients)}")
print(f"Email sent to {EMAIL_TO} with attachment: {filename}")
